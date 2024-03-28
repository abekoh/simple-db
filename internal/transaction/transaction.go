package transaction

import (
	"fmt"
	"sync/atomic"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
)

var transactionNumberSequence atomic.Int32

func nextTxNumber() int32 {
	return transactionNumberSequence.Add(1)
}

type Transaction struct {
	bm     *buffer.Manager
	fm     *file.Manager
	lm     *log.Manager
	txNum  int32
	bufMap map[file.BlockID]*buffer.Buffer
}

func NewTransaction(
	bm *buffer.Manager,
	fm *file.Manager,
	lm *log.Manager,
) *Transaction {
	return &Transaction{
		bm:     bm,
		fm:     fm,
		lm:     lm,
		txNum:  nextTxNumber(),
		bufMap: make(map[file.BlockID]*buffer.Buffer),
	}
}

func (t *Transaction) Commit() error {
	if err := t.bm.FlushAll(t.txNum); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	lsn, err := NewCommitLogRecord(t.txNum).WriteTo(t.lm)
	if err != nil {
		return fmt.Errorf("could not write log: %w", err)
	}
	if err := t.lm.Flush(lsn); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	// TODO: release locks
	t.unpinAll()
	return nil
}

func (t *Transaction) Rollback() error {
	for r := range t.lm.Iterator() {
		lr := CreateLogRecord(r)
		if lr == nil {
			return fmt.Errorf("could not create log record")
		}
		if lr.TxNum() == t.txNum {
			if lr.Type() == Start {
				return nil
			}
			if err := lr.Undo(t); err != nil {
				return fmt.Errorf("could not undo: %w", err)
			}
		}
	}
	return nil
}

func (t *Transaction) Pin(blockID file.BlockID) (*buffer.Buffer, error) {
	buf, err := t.bm.Pin(blockID)
	if err != nil {
		return nil, fmt.Errorf("could not pin: %w", err)
	}
	t.bufMap[blockID] = buf
	return buf, nil
}

func (t *Transaction) Unpin(buf *buffer.Buffer) {
	t.bm.Unpin(buf)
	delete(t.bufMap, buf.BlockID())
}

func (t *Transaction) unpinAll() {
	for _, buf := range t.bufMap {
		t.bm.Unpin(buf)
	}
	t.bufMap = make(map[file.BlockID]*buffer.Buffer)
}

func (t *Transaction) Int32(blockID file.BlockID, offset int32) (int32, error) {
	// TODO: sLock
	buf, ok := t.bufMap[blockID]
	if !ok {
		return 0, fmt.Errorf("block not pinned")
	}
	return buf.Page().Int32(offset), nil
}

func (t *Transaction) Str(blockID file.BlockID, offset int32) (string, error) {
	// TODO: sLock
	buf, ok := t.bufMap[blockID]
	if !ok {
		return "", fmt.Errorf("block not pinned")
	}
	return buf.Page().Str(offset), nil
}

func (t *Transaction) SetInt32(blockID file.BlockID, offset, val int32, okToLog bool) error {
	// TODO: xLock
	buf, ok := t.bufMap[blockID]
	if !ok {
		return fmt.Errorf("block not pinned")
	}
	lsn := log.SequenceNumber(-1)
	if okToLog {
		oldVal := buf.Page().Int32(offset)
		blockID := buf.BlockID()
		newLsn, err := NewSetInt32LogRecord(t.txNum, blockID, offset, oldVal).WriteTo(t.lm)
		if err != nil {
			return fmt.Errorf("could not write log: %w", err)
		}
		lsn = newLsn
	}
	buf.Page().SetInt32(offset, val)
	buf.SetModified(t.txNum, lsn)
	return nil
}

func (t *Transaction) SetStr(blockID file.BlockID, offset int32, val string, okToLog bool) error {
	// TODO: xLock
	buf, ok := t.bufMap[blockID]
	if !ok {
		return fmt.Errorf("block not pinned")
	}
	lsn := log.SequenceNumber(-1)
	if okToLog {
		oldVal := buf.Page().Str(offset)
		blockID := buf.BlockID()
		newLsn, err := NewSetStringLogRecord(t.txNum, blockID, offset, oldVal).WriteTo(t.lm)
		if err != nil {
			return fmt.Errorf("could not write log: %w", err)
		}
		lsn = newLsn
	}
	buf.Page().SetStr(offset, val)
	buf.SetModified(t.txNum, lsn)
	return nil
}

func (t *Transaction) Size(filename string) (int32, error) {
	// sLock
	l, err := t.fm.Length(filename)
	if err != nil {
		return 0, fmt.Errorf("could not get length: %w", err)
	}
	return l, nil
}

func (t *Transaction) Append(filename string) (file.BlockID, error) {
	// xLock
	blockID, err := t.fm.Append(filename)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("could not append: %w", err)
	}
	return blockID, nil
}

func (t *Transaction) BlockSize() int32 {
	return t.fm.BlockSize()
}

func (t *Transaction) AvailableBuffersNum() int {
	return t.bm.AvailableNum()
}

type LogRecordType int32

const (
	Checkpoint LogRecordType = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

type LogRecord interface {
	fmt.Stringer
	Type() LogRecordType
	TxNum() int32
	Undo(tx *Transaction) error
	WriteTo(lm *log.Manager) (log.SequenceNumber, error)
}

func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageBytes(bytes)
	switch LogRecordType(p.Int32(0)) {
	case Checkpoint:
		return NewCheckpointLogRecord()
	case Start:
		return NewStartLogRecordPage(p)
	case Commit:
		return NewCommitLogRecordPage(p)
	case Rollback:
		return NewRollbackLogRecordPage(p)
	case SetInt:
		return NewSetInt32LogRecordPage(p)
	case SetString:
		return NewSetStringLogRecordPage(p)
	default:
		return nil
	}
}

type CheckpointLogRecord struct {
}

func NewCheckpointLogRecord() CheckpointLogRecord {
	return CheckpointLogRecord{}
}

func (r CheckpointLogRecord) String() string {
	return "<CHECKPOINT>"
}

func (r CheckpointLogRecord) Type() LogRecordType {
	return Checkpoint
}

func (r CheckpointLogRecord) TxNum() int32 {
	return -1
}

func (r CheckpointLogRecord) Undo(tx *Transaction) error {
	return nil
}

func (r CheckpointLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	p := file.NewPageBytes(make([]byte, 4))
	p.SetInt32(0, int32(Checkpoint))
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

type StartLogRecord struct {
	txNum int32
}

func NewStartLogRecord(txNum int32) StartLogRecord {
	return StartLogRecord{
		txNum: txNum,
	}
}

func NewStartLogRecordPage(p *file.Page) StartLogRecord {
	return StartLogRecord{
		txNum: p.Int32(4),
	}
}

func (r StartLogRecord) String() string {
	return fmt.Sprintf("<START %d >", r.txNum)
}

func (r StartLogRecord) TxNum() int32 {
	return r.txNum
}

func (r StartLogRecord) Type() LogRecordType {
	return Start
}

func (r StartLogRecord) Undo(tx *Transaction) error {
	return nil
}

func (r StartLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	p := file.NewPageBytes(make([]byte, 8))
	p.SetInt32(0, int32(Start))
	p.SetInt32(4, r.txNum)
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

type CommitLogRecord struct {
	txNum int32
}

func NewCommitLogRecord(txNum int32) CommitLogRecord {
	return CommitLogRecord{
		txNum: txNum,
	}
}

func NewCommitLogRecordPage(p *file.Page) CommitLogRecord {
	return CommitLogRecord{
		txNum: p.Int32(4),
	}
}

func (r CommitLogRecord) String() string {
	return fmt.Sprintf("<COMMIT %d >", r.txNum)
}

func (r CommitLogRecord) TxNum() int32 {
	return r.txNum
}

func (r CommitLogRecord) Type() LogRecordType {
	return Commit
}

func (r CommitLogRecord) Undo(tx *Transaction) error {
	return nil
}

func (r CommitLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	p := file.NewPageBytes(make([]byte, 8))
	p.SetInt32(0, int32(Commit))
	p.SetInt32(4, r.txNum)
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

type RollbackLogRecord struct {
	txNum int32
}

func NewRollbackLogRecord(txNum int32) RollbackLogRecord {
	return RollbackLogRecord{
		txNum: txNum,
	}
}

func NewRollbackLogRecordPage(p *file.Page) RollbackLogRecord {
	return RollbackLogRecord{
		txNum: p.Int32(4),
	}
}

func (r RollbackLogRecord) String() string {
	return fmt.Sprintf("<ROLLBACK %d >", r.txNum)
}

func (r RollbackLogRecord) TxNum() int32 {
	return r.txNum
}

func (r RollbackLogRecord) Type() LogRecordType {
	return Rollback
}

func (r RollbackLogRecord) Undo(tx *Transaction) error {
	return nil
}

func (r RollbackLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	p := file.NewPageBytes(make([]byte, 8))
	p.SetInt32(0, int32(Rollback))
	p.SetInt32(4, r.txNum)
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

type SetInt32LogRecord struct {
	txNum   int32
	offset  int32
	val     int32
	blockID file.BlockID
}

func NewSetInt32LogRecord(txNum int32, blockID file.BlockID, offset, val int32) SetInt32LogRecord {
	return SetInt32LogRecord{
		txNum:   txNum,
		blockID: blockID,
		offset:  offset,
		val:     val,
	}
}

func NewSetInt32LogRecordPage(p *file.Page) SetInt32LogRecord {
	const tpos = 4
	txNum := p.Int32(tpos)
	const fpos = tpos + 4
	filename := p.Str(fpos)
	bpos := fpos + file.PageStrMaxLength(filename)
	blockID := file.NewBlockID(filename, p.Int32(bpos))
	opos := bpos + 4
	offset := p.Int32(opos)
	vpos := opos + 4
	val := p.Int32(vpos)
	return SetInt32LogRecord{
		txNum:   txNum,
		blockID: blockID,
		offset:  offset,
		val:     val,
	}
}

func (r SetInt32LogRecord) String() string {
	return fmt.Sprintf("<SETINT %d %s %d %d>", r.txNum, r.blockID, r.offset, r.val)
}

func (r SetInt32LogRecord) TxNum() int32 {
	return r.txNum
}

func (r SetInt32LogRecord) Type() LogRecordType {
	return SetInt
}

func (r SetInt32LogRecord) Undo(tx *Transaction) error {
	buf, err := tx.Pin(r.blockID)
	if err != nil {
		return fmt.Errorf("could not pin: %w", err)
	}
	if err := tx.SetInt32(r.blockID, r.offset, r.val, false); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	tx.Unpin(buf)
	return nil
}

func (r SetInt32LogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	const tpos = 4
	const fpos = tpos + 4
	bpos := fpos + file.PageStrMaxLength(r.blockID.Filename())
	opos := bpos + 4
	vpos := opos + 4
	p := file.NewPageBytes(make([]byte, vpos+4))
	p.SetInt32(0, int32(SetInt))
	p.SetInt32(tpos, r.txNum)
	p.SetStr(fpos, r.blockID.Filename())
	p.SetInt32(opos, r.offset)
	p.SetInt32(vpos, r.val)
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

type SetStringLogRecord struct {
	txNum   int32
	offset  int32
	val     string
	blockID file.BlockID
}

func NewSetStringLogRecord(txNum int32, blockID file.BlockID, offset int32, val string) SetStringLogRecord {
	return SetStringLogRecord{
		txNum:   txNum,
		blockID: blockID,
		offset:  offset,
		val:     val,
	}
}

func NewSetStringLogRecordPage(p *file.Page) SetStringLogRecord {
	const tpos = 4
	txNum := p.Int32(tpos)
	const fpos = tpos + 4
	filename := p.Str(fpos)
	bpos := fpos + file.PageStrMaxLength(filename)
	blockID := file.NewBlockID(filename, p.Int32(bpos))
	opos := bpos + 4
	offset := p.Int32(opos)
	vpos := opos + 4
	val := p.Str(vpos)
	return SetStringLogRecord{
		txNum:   txNum,
		blockID: blockID,
		offset:  offset,
		val:     val,
	}
}

func (r SetStringLogRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %s %d %s>", r.txNum, r.blockID, r.offset, r.val)
}

func (r SetStringLogRecord) TxNum() int32 {
	return r.txNum
}

func (r SetStringLogRecord) Type() LogRecordType {
	return SetString
}

func (r SetStringLogRecord) Undo(tx *Transaction) error {
	buf, err := tx.Pin(r.blockID)
	if err != nil {
		return fmt.Errorf("could not pin: %w", err)
	}
	if err := tx.SetStr(r.blockID, r.offset, r.val, false); err != nil {
		return fmt.Errorf("could not set string: %w", err)
	}
	tx.Unpin(buf)
	return nil
}

func (r SetStringLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	const tpos = 4
	const fpos = tpos + 4
	bpos := fpos + file.PageStrMaxLength(r.blockID.Filename())
	opos := bpos + 4
	vpos := opos + 4
	p := file.NewPageBytes(make([]byte, vpos+file.PageStrMaxLength(r.val)))
	p.SetInt32(0, int32(SetString))
	p.SetInt32(tpos, r.txNum)
	p.SetStr(fpos, r.blockID.Filename())
	p.SetInt32(bpos, r.blockID.Num())
	p.SetInt32(opos, r.offset)
	p.SetStr(vpos, r.val)
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}
