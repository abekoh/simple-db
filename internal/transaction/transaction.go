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
			lr.Undo()
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
	Undo()
	WriteTo(lm *log.Manager) (log.SequenceNumber, error)
}

func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageBytes(bytes)
	switch LogRecordType(p.Int32(0)) {
	case Commit:
		return NewCommitLogRecordPage(p)
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

func (r CheckpointLogRecord) Undo() {
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

func (r StartLogRecord) Undo() {
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

func (r CommitLogRecord) Undo() {
}

func (r CommitLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	p := file.NewPageBytes(make([]byte, 2))
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

func (r RollbackLogRecord) Undo() {
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
