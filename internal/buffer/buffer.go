package buffer

import (
	"fmt"
	"sync/atomic"
	"time"

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
	if b.pinsCount <= 0 {
		panic("unpin: pinsCount is already 0")
	}
	b.pinsCount--
}

type Manager struct {
	pool         map[file.BlockID]*Buffer
	availableNum atomic.Int32
	pinRequestCh chan pinRequest
	unpinCh      chan *Buffer
}

type pinRequest struct {
	BlockID   file.BlockID
	ReceiveCh chan *Buffer
}

func NewManager(fm *file.Manager, lm *log.Manager, buffNum int) *Manager {
	pool := make(map[file.BlockID]*Buffer, buffNum)
	for i := 0; i < buffNum; i++ {
		b := NewBuffer(fm, lm)
		pool[b.blockID] = b
	}
	availableNum := atomic.Int32{}
	availableNum.Store(int32(buffNum))
	m := &Manager{
		pool:         pool,
		availableNum: availableNum,
		pinRequestCh: make(chan pinRequest),
	}
	go m.loop()
	return m
}

func (m *Manager) loop() {
	waitMap := make(map[file.BlockID][]chan *Buffer)
	for {
		select {
		case b := <-m.unpinCh:
			b.unpin()
			if !b.IsPinned() {
				m.availableNum.Add(1)
			}
			if len(waitMap[b.blockID]) > 0 {
				b.pin()
				waitMap[b.blockID][0] <- b
				waitMap[b.blockID] = waitMap[b.blockID][1:]
			}
		case pinReq := <-m.pinRequestCh:
			b, ok := m.pool[pinReq.BlockID]
			if ok {
				b.pin()
				pinReq.ReceiveCh <- b
			} else {
				received := false
				for _, b := range m.pool {
					if !b.IsPinned() {
						b.assignedToBlock(pinReq.BlockID)
						b.pin()
						m.availableNum.Add(-1)
						pinReq.ReceiveCh <- b
						received = true
						break
					}
				}
				if !received {
					if _, ok := waitMap[pinReq.BlockID]; !ok {
						waitMap[pinReq.BlockID] = []chan *Buffer{}
					} else {
						waitMap[pinReq.BlockID] = append(waitMap[pinReq.BlockID], pinReq.ReceiveCh)
					}
				}
			}
		}
	}
}

func (m *Manager) AvailableNum() int {
	return int(m.availableNum.Load())
}

func (m *Manager) FlushAll(txNum TransactionNumber) error {
	// TODO: Implement this
	return nil
}

const maxWaitTime = 10 * time.Second

func (m *Manager) Pin(blockID file.BlockID) (*Buffer, error) {
	ch := make(chan *Buffer)
	m.pinRequestCh <- pinRequest{BlockID: blockID, ReceiveCh: ch}
	select {
	case b := <-ch:
		return b, nil
	case <-time.After(maxWaitTime):
		return nil, fmt.Errorf("could not pin %s", blockID)
	}
}

func (m *Manager) Unpin(b *Buffer) {
	m.unpinCh <- b
}
