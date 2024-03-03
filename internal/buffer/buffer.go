package buffer

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
)

type Buffer struct {
	fm        *file.Manager
	lm        *log.Manager
	page      *file.Page
	blockID   file.BlockID
	pinsCount int32
	txNum     int32
	lsn       log.SequenceNumber
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	return &Buffer{
		fm:    fm,
		lm:    lm,
		page:  file.NewPage(fm.BlockSize()),
		txNum: -1,
		lsn:   -1,
	}
}

func (b *Buffer) Page() *file.Page {
	return b.page
}

func (b *Buffer) BlockID() file.BlockID {
	return b.blockID
}

func (b *Buffer) TxNum() int32 {
	return b.txNum
}

func (b *Buffer) SetModified(txNum int32, lsn log.SequenceNumber) {
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
	if b.txNum < 0 {
		return nil
	}
	if err := b.lm.Flush(b.lsn); err != nil {
		return fmt.Errorf("could not flush: %w", err)
	}
	if err := b.fm.Write(b.blockID, b.page); err != nil {
		return fmt.Errorf("could not write: %w", err)
	}
	b.txNum = -1
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

type (
	Manager struct {
		pool         []*Buffer
		availableNum atomic.Int32
		pinRequestCh chan pinRequest
		unpinCh      chan unpinRequest
		flushAllCh   chan chan<- error
		maxWaitTime  time.Duration
	}
	ManagerOption func(*Manager)
)

type (
	bufferResult struct {
		buf *Buffer
		err error
	}
	pinRequest struct {
		blockID   file.BlockID
		receiveCh chan<- bufferResult
		cancelCh  <-chan struct{}
	}
	unpinRequest struct {
		buf        *Buffer
		completeCh chan<- struct{}
	}
)

const defaultMaxWaitTime = 10 * time.Second

func NewManager(
	fm *file.Manager,
	lm *log.Manager,
	buffNum int,
	opts ...ManagerOption,
) *Manager {
	pool := make([]*Buffer, buffNum)
	for i := range pool {
		pool[i] = NewBuffer(fm, lm)
	}
	availableNum := atomic.Int32{}
	availableNum.Store(int32(buffNum))
	m := &Manager{
		pool:         pool,
		availableNum: availableNum,
		pinRequestCh: make(chan pinRequest),
		unpinCh:      make(chan unpinRequest),
		flushAllCh:   make(chan chan<- error),
		maxWaitTime:  defaultMaxWaitTime,
	}
	for _, opt := range opts {
		opt(m)
	}
	go m.loop()
	return m
}

func WithMaxWaitTime(d time.Duration) ManagerOption {
	return func(m *Manager) {
		m.maxWaitTime = d
	}
}

func (m *Manager) loop() {
	waitMap := make(map[file.BlockID][]pinRequest)
	for {
		select {
		case errCh := <-m.flushAllCh:
			var err error
			for _, b := range m.pool {
				if err := b.flush(); err != nil {
					err = fmt.Errorf("could not flush: %w", err)
					break
				}
			}
			errCh <- err
		case unpinReq := <-m.unpinCh:
			unpinReq.buf.unpin()
			if len(waitMap[unpinReq.buf.blockID]) > 0 {
				unpinReq.buf.pin()
				req := waitMap[unpinReq.buf.blockID][0]
				select {
				case <-req.cancelCh:
				default:
					req.receiveCh <- bufferResult{buf: unpinReq.buf}
				}
				if len(waitMap[unpinReq.buf.blockID]) > 1 {
					waitMap[unpinReq.buf.blockID] = waitMap[unpinReq.buf.blockID][1:]
				} else {
					delete(waitMap, unpinReq.buf.blockID)
				}
			} else if !unpinReq.buf.IsPinned() {
				m.availableNum.Add(1)
			}
			unpinReq.completeCh <- struct{}{}
		case pinReq := <-m.pinRequestCh:
			var b *Buffer
			for _, buf := range m.pool {
				if buf.blockID == pinReq.blockID {
					b = buf
					break
				}
			}
			if b != nil {
				if !b.IsPinned() {
					m.availableNum.Add(-1)
				}
				b.pin()
				pinReq.receiveCh <- bufferResult{buf: b}
			} else {
				received := false
				for _, b := range m.pool {
					if !b.IsPinned() {
						err := b.assignedToBlock(pinReq.blockID)
						if err != nil {
							pinReq.receiveCh <- bufferResult{err: err}
							received = true
							break
						}
						b.pin()
						m.availableNum.Add(-1)
						pinReq.receiveCh <- bufferResult{buf: b}
						received = true
						break
					}
				}
				if !received {
					select {
					case <-pinReq.cancelCh:
						continue
					default:
					}
					if _, ok := waitMap[pinReq.blockID]; !ok {
						waitMap[pinReq.blockID] = []pinRequest{pinReq}
					} else {
						waitMap[pinReq.blockID] = append(waitMap[pinReq.blockID], pinReq)
					}
				}
			}
		}
	}
}

func (m *Manager) AvailableNum() int {
	return int(m.availableNum.Load())
}

func (m *Manager) FlushAll(txNum int32) error {
	ch := make(chan error)
	m.flushAllCh <- ch
	return <-ch
}

func (m *Manager) Pin(blockID file.BlockID) (*Buffer, error) {
	resCh := make(chan bufferResult)
	cancelCh := make(chan struct{}, 1)
	m.pinRequestCh <- pinRequest{blockID: blockID, receiveCh: resCh, cancelCh: cancelCh}
	select {
	case res := <-resCh:
		return res.buf, res.err
	case <-time.After(m.maxWaitTime):
		cancelCh <- struct{}{}
		return nil, fmt.Errorf("could not pin %v", blockID)
	}
}

func (m *Manager) Unpin(b *Buffer) {
	ch := make(chan struct{})
	m.unpinCh <- unpinRequest{buf: b, completeCh: ch}
	<-ch
}
