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
	Undo() error
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

type logRecord struct {
	txNum int32
}

func (r logRecord) TxNum() int32 {
	return r.txNum
}

type CommitLogRecord struct {
	logRecord
}

func NewCommitLogRecord(txNum int32) CommitLogRecord {
	return CommitLogRecord{
		logRecord: logRecord{txNum: txNum},
	}
}

func NewCommitLogRecordPage(p *file.Page) CommitLogRecord {
	return CommitLogRecord{
		logRecord: logRecord{txNum: p.Int32(4)},
	}
}

func (r CommitLogRecord) String() string {
	return fmt.Sprintf("<COMMIT %d >", r.txNum)
}

func (r CommitLogRecord) Type() LogRecordType {
	return Commit
}

func (r CommitLogRecord) Undo() error {
	return nil
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
