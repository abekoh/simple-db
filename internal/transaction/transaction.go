package transaction

import (
	"fmt"
	"log/slog"
	"slices"
	"sync"
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
	bm      *buffer.Manager
	fm      *file.Manager
	lm      *log.Manager
	txNum   int32
	bufMap  map[file.BlockID]*buffer.Buffer
	bufPins []file.BlockID
	concMgr concurrencyManager
}

func NewTransaction(
	bm *buffer.Manager,
	fm *file.Manager,
	lm *log.Manager,
) (*Transaction, error) {
	txNum := nextTxNumber()
	if _, err := NewStartLogRecord(txNum).WriteTo(lm); err != nil {
		return nil, fmt.Errorf("could not write log: %w", err)
	}
	return &Transaction{
		bm:      bm,
		fm:      fm,
		lm:      lm,
		txNum:   txNum,
		bufMap:  make(map[file.BlockID]*buffer.Buffer),
		bufPins: make([]file.BlockID, 0),
		concMgr: newConcurrencyManager(),
	}, nil
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
	t.concMgr.release()
	t.unpinAll()
	return nil
}

func (t *Transaction) Rollback() error {
	for r := range t.lm.Iterator() {
		lr := NewLogRecord(r)
		if lr == nil {
			return fmt.Errorf("could not create log record")
		}
		if lr.TxNum() == t.txNum {
			if lr.Type() == Start {
				break
			}
			if err := lr.Undo(t); err != nil {
				return fmt.Errorf("could not undo: %w", err)
			}
		}
	}
	t.bm.FlushAll(t.txNum)
	lsn, err := NewRollbackLogRecord(t.txNum).WriteTo(t.lm)
	if err != nil {
		return fmt.Errorf("could not write log: %w", err)
	}
	if err := t.lm.Flush(lsn); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	t.concMgr.release()
	t.unpinAll()
	return nil
}

func (t *Transaction) Pin(blockID file.BlockID) (*buffer.Buffer, error) {
	buf, err := t.bm.Pin(blockID)
	if err != nil {
		return nil, fmt.Errorf("could not pin: %w", err)
	}
	t.bufMap[blockID] = buf
	t.bufPins = append(t.bufPins, blockID)
	return buf, nil
}

func (t *Transaction) Unpin(buf *buffer.Buffer) {
	t.bm.Unpin(buf)
	if idx := slices.Index(t.bufPins, buf.BlockID()); idx != -1 {
		t.bufPins = append(t.bufPins[:idx], t.bufPins[idx+1:]...)
	}
	if !slices.Contains(t.bufPins, buf.BlockID()) {
		delete(t.bufMap, buf.BlockID())
	}
}

func (t *Transaction) unpinAll() {
	for _, buf := range t.bufMap {
		t.bm.Unpin(buf)
	}
	t.bufMap = make(map[file.BlockID]*buffer.Buffer)
	t.bufPins = make([]file.BlockID, 0)
}

func (t *Transaction) Int32(blockID file.BlockID, offset int32) (int32, error) {
	t.concMgr.sLock(blockID)
	buf, ok := t.bufMap[blockID]
	if !ok {
		return 0, fmt.Errorf("block not pinned")
	}
	return buf.Page().Int32(offset), nil
}

func (t *Transaction) Str(blockID file.BlockID, offset int32) (string, error) {
	t.concMgr.sLock(blockID)
	buf, ok := t.bufMap[blockID]
	if !ok {
		return "", fmt.Errorf("block not pinned")
	}
	return buf.Page().Str(offset), nil
}

func (t *Transaction) SetInt32(blockID file.BlockID, offset, val int32, okToLog bool) error {
	t.concMgr.xLock(blockID)
	buf, ok := t.bufMap[blockID]
	if !ok {
		return fmt.Errorf("block not pinned")
	}
	lsn := log.SequenceNumber(-1)
	if okToLog {
		oldVal := buf.Page().Int32(offset)
		blockID := buf.BlockID()
		newLSN, err := NewSetInt32LogRecord(t.txNum, blockID, offset, oldVal).WriteTo(t.lm)
		if err != nil {
			return fmt.Errorf("could not write log: %w", err)
		}
		lsn = newLSN
	}
	buf.Page().SetInt32(offset, val)
	buf.SetModified(t.txNum, lsn)
	return nil
}

func (t *Transaction) SetStr(blockID file.BlockID, offset int32, val string, okToLog bool) error {
	t.concMgr.xLock(blockID)
	buf, ok := t.bufMap[blockID]
	if !ok {
		return fmt.Errorf("block not pinned")
	}
	lsn := log.SequenceNumber(-1)
	if okToLog {
		oldVal := buf.Page().Str(offset)
		blockID := buf.BlockID()
		newLSN, err := NewSetStrLogRecord(t.txNum, blockID, offset, oldVal).WriteTo(t.lm)
		if err != nil {
			return fmt.Errorf("could not write log: %w", err)
		}
		lsn = newLSN
	}
	buf.Page().SetStr(offset, val)
	buf.SetModified(t.txNum, lsn)
	return nil
}

func (t *Transaction) Size(filename string) (int32, error) {
	dummyBlockID := file.NewBlockID(filename, -1)
	t.concMgr.sLock(dummyBlockID)
	l, err := t.fm.Length(filename)
	if err != nil {
		return 0, fmt.Errorf("could not get length: %w", err)
	}
	return l, nil
}

func (t *Transaction) Append(filename string) (file.BlockID, error) {
	dummyBlockID := file.NewBlockID(filename, -1)
	t.concMgr.xLock(dummyBlockID)
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
	SetInt32
	SetStr
)

type LogRecord interface {
	fmt.Stringer
	Type() LogRecordType
	TxNum() int32
	Undo(tx *Transaction) error
	WriteTo(lm *log.Manager) (log.SequenceNumber, error)
}

func NewLogRecord(bytes []byte) LogRecord {
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
	case SetInt32:
		return NewSetInt32LogRecordPage(p)
	case SetStr:
		return NewSetStrLogRecordPage(p)
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
	slog.Debug("write checkpoint log", "record", r)
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
	return fmt.Sprintf("<START %d>", r.txNum)
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
	slog.Debug("write start log", "record", r)
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
	return fmt.Sprintf("<COMMIT %d>", r.txNum)
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
	slog.Debug("write commit log", "record", r)
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
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
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
	slog.Debug("write rollback log", "record", r)
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
	return fmt.Sprintf("<SETINT32 %d %s %d %d>", r.txNum, r.blockID, r.offset, r.val)
}

func (r SetInt32LogRecord) TxNum() int32 {
	return r.txNum
}

func (r SetInt32LogRecord) Type() LogRecordType {
	return SetInt32
}

func (r SetInt32LogRecord) Undo(tx *Transaction) error {
	slog.Debug("undo setint32 log", "record", r)
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
	slog.Debug("write setint32 log", "record", r)
	const tpos = 4
	const fpos = tpos + 4
	bpos := fpos + file.PageStrMaxLength(r.blockID.Filename())
	opos := bpos + 4
	vpos := opos + 4
	p := file.NewPageBytes(make([]byte, vpos+4))
	p.SetInt32(0, int32(SetInt32))
	p.SetInt32(tpos, r.txNum)
	p.SetStr(fpos, r.blockID.Filename())
	p.SetInt32(bpos, r.blockID.Num())
	p.SetInt32(opos, r.offset)
	p.SetInt32(vpos, r.val)
	lsn, err := lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

type SetStrLogRecord struct {
	txNum   int32
	offset  int32
	val     string
	blockID file.BlockID
}

func NewSetStrLogRecord(txNum int32, blockID file.BlockID, offset int32, val string) SetStrLogRecord {
	return SetStrLogRecord{
		txNum:   txNum,
		blockID: blockID,
		offset:  offset,
		val:     val,
	}
}

func NewSetStrLogRecordPage(p *file.Page) SetStrLogRecord {
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
	return SetStrLogRecord{
		txNum:   txNum,
		blockID: blockID,
		offset:  offset,
		val:     val,
	}
}

func (r SetStrLogRecord) String() string {
	return fmt.Sprintf("<SETSTR %d %s %d %s>", r.txNum, r.blockID, r.offset, r.val)
}

func (r SetStrLogRecord) TxNum() int32 {
	return r.txNum
}

func (r SetStrLogRecord) Type() LogRecordType {
	return SetStr
}

func (r SetStrLogRecord) Undo(tx *Transaction) error {
	slog.Debug("undo setstr log", "record", r)
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

func (r SetStrLogRecord) WriteTo(lm *log.Manager) (log.SequenceNumber, error) {
	slog.Debug("write setst log", "record", r)
	const tpos = 4
	const fpos = tpos + 4
	bpos := fpos + file.PageStrMaxLength(r.blockID.Filename())
	opos := bpos + 4
	vpos := opos + 4
	p := file.NewPageBytes(make([]byte, vpos+file.PageStrMaxLength(r.val)))
	p.SetInt32(0, int32(SetStr))
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

var globalLockTable sync.Map

type (
	sLockRequest struct {
		blockID  file.BlockID
		complete chan error
	}
	xLockRequest struct {
		blockID  file.BlockID
		complete chan error
	}
	concurrencyManager struct {
		sLockCh   chan sLockRequest
		xLockCh   chan xLockRequest
		releaseCh chan struct{}
	}
)

func newConcurrencyManager() concurrencyManager {
	m := concurrencyManager{
		sLockCh:   make(chan sLockRequest),
		xLockCh:   make(chan xLockRequest),
		releaseCh: make(chan struct{}),
	}
	go m.loop()
	return m
}

func (m *concurrencyManager) sLock(blockID file.BlockID) {
	ch := make(chan error)
	m.sLockCh <- sLockRequest{
		blockID:  blockID,
		complete: ch,
	}
	<-ch
}

func (m *concurrencyManager) xLock(blockID file.BlockID) {
	ch := make(chan error)
	m.xLockCh <- xLockRequest{
		blockID:  blockID,
		complete: ch,
	}
	<-ch
}

func (m *concurrencyManager) release() {
	m.releaseCh <- struct{}{}
}

func (m *concurrencyManager) loop() {
	type lockType int
	const (
		sLock lockType = iota
		xLock
	)
	localLockTable := make(map[file.BlockID]lockType)
	for {
		select {
		case req := <-m.sLockCh:
			if t, ok := localLockTable[req.blockID]; ok {
				switch t {
				case sLock:
					req.complete <- nil
				case xLock:
					req.complete <- fmt.Errorf("block already locked")
				}
				continue
			}
			mutex, _ := globalLockTable.LoadOrStore(req.blockID, &sync.RWMutex{})
			if mutex.(*sync.RWMutex).TryRLock() {
				localLockTable[req.blockID] = sLock
				continue
			}
			req.complete <- fmt.Errorf("block already locked")
		case req := <-m.xLockCh:
			if t, ok := localLockTable[req.blockID]; ok {
				switch t {
				case sLock:
					mutex, ok := globalLockTable.Load(req.blockID)
					if !ok {
						req.complete <- fmt.Errorf("block not found")
						continue
					}
					mutex.(*sync.RWMutex).RUnlock()
					mutex.(*sync.RWMutex).Lock()
					localLockTable[req.blockID] = xLock
				case xLock:
					req.complete <- nil
				}
				continue
			}
			mutex, _ := globalLockTable.LoadOrStore(req.blockID, &sync.RWMutex{})
			if mutex.(*sync.RWMutex).TryLock() {
				localLockTable[req.blockID] = xLock
				continue
			}
			req.complete <- fmt.Errorf("block already locked")
		case <-m.releaseCh:
			for blockID, t := range localLockTable {
				mutex, ok := globalLockTable.Load(blockID)
				if !ok {
					continue
				}
				switch t {
				case sLock:
					mutex.(*sync.RWMutex).RUnlock()
				case xLock:
					mutex.(*sync.RWMutex).Unlock()
				}
			}
			localLockTable = make(map[file.BlockID]lockType)
		}
	}
}
