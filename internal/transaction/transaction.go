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
	lsn, err := t.writeLog(t.txNum)
	if err != nil {
		return fmt.Errorf("could not write log: %w", err)
	}
	if err := t.lm.Flush(lsn); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	t.unpinAll()
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

func (t *Transaction) writeLog(txNum int32) (log.SequenceNumber, error) {
	p := file.NewPageBytes(make([]byte, 2))
	p.SetInt32(0, Commit)
	p.SetInt32(4, txNum)
	lsn, err := t.lm.Append(p.RawBytes())
	if err != nil {
		return 0, fmt.Errorf("could not append: %w", err)
	}
	return lsn, nil
}

const (
	Checkpoint int32 = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)
