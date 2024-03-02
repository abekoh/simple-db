package buffer

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
)

type TransactionNumber struct {
	Value int32
	Valid bool
}

func NewNullTransactionNumber() TransactionNumber {
	return TransactionNumber{Valid: false}
}

func NewNotNullTransactionNumber(v int32) TransactionNumber {
	return TransactionNumber{Value: v, Valid: true}
}

type Buffer struct {
	fm        *file.Manager
	lm        *log.Manager
	page      *file.Page
	blockID   file.BlockID
	pinsCount int32
	txNum     TransactionNumber
	lsn       log.SequenceNumber
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		fm:    fm,
		lm:    lm,
		page:  file.NewPage(fm.BlockSize()),
		txNum: NewNullTransactionNumber(),
		lsn:   -1,
	}
}

func (b *Buffer) Page() *file.Page {
	return b.page
}

func (b *Buffer) BlockID() file.BlockID {
	return b.blockID
}

func (b *Buffer) TxNum() TransactionNumber {
	return b.txNum
}

func (b *Buffer) SetModified(txNum TransactionNumber, lsn log.SequenceNumber) {
	b.txNum = txNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pinsCount > 0
}

func (b *Buffer) assignedToBlock(blockID file.BlockID) error {
	if err := b.flush(); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	b.blockID = blockID
	if err := b.fm.Read(blockID, b.page); err != nil {
		return fmt.Errorf("could not read: %w", err)
	}
	b.pinsCount = 0
	return nil
}

func (b *Buffer) flush() error {
	if !b.txNum.Valid {
		return nil
	}
	if err := b.lm.Flush(b.lsn); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	if err := b.fm.Write(b.blockID, b.page); err != nil {
		return fmt.Errorf("could not write: %w", err)
	}
	b.txNum = NewNullTransactionNumber()
	return nil
}

func (b *Buffer) pin() {
	b.pinsCount++
}

func (b *Buffer) unpin() {
	b.pinsCount--
}

type Manager struct {
	pool []*Buffer
}

func NewManager(fm *file.Manager, lm *log.Manager, buffNum int32) *Manager {
	pool := make([]*Buffer, buffNum)
	for i := range pool {
		pool[i] = NewBuffer(fm, lm)
	}
	return &Manager{
		pool: pool,
	}
}

func (m *Manager) AvailableNum() int {
	return len(m.pool)
}

func (m *Manager) FlushAll(txNum TransactionNumber) error {
	if !txNum.Valid {
		return fmt.Errorf("invalid txNum")
	}
	for _, buff := range m.pool {
		if buff.TxNum() == txNum {
			if err := buff.flush(); err != nil {
				return fmt.Errorf("could not flush: %w", err)
			}
		}
	}
	return nil
}
