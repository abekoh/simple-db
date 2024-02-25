package log

import (
	"fmt"
	"sync"

	"github.com/abekoh/simple-db/internal/file"
)

type Manager struct {
	fm             *file.Manager
	filename       string
	page           *file.Page
	currentBlockID file.BlockID
	latestLSN      int32
	lastSavedLSN   int32
	appendMu       sync.Mutex
}

func NewManager(fm *file.Manager, filename string) (*Manager, error) {
	p := file.NewPage(fm.BlockSize())
	m := &Manager{
		fm:       fm,
		filename: filename,
		page:     p,
	}
	logSize, err := fm.Length(filename)
	if err != nil {
		return nil, fmt.Errorf("could not get length: %w", err)
	}
	if logSize == 0 {
		currentBlockID, err := m.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("could not append new block: %w", err)
		}
		m.currentBlockID = currentBlockID
	} else {
		m.currentBlockID = file.NewBlockID(filename, logSize-1)
		err = fm.Read(m.currentBlockID, m.page)
		if err != nil {
			return nil, fmt.Errorf("could not read: %w", err)
		}
	}
	return m, nil
}

func (m *Manager) Flush(lsn int32) error {
	if lsn >= m.latestLSN {
		return m.flush()
	}
	return nil
}

func (m *Manager) Append(rec []byte) (lsn int32, err error) {
	m.appendMu.Lock()
	defer m.appendMu.Unlock()
	boundary := m.page.Int32(0)
	recSize := int32(len(rec))
	needBytes := recSize + 4
	if boundary-needBytes < 4 {
		if err := m.flush(); err != nil {
			return 0, fmt.Errorf("could not flush: %w", err)
		}
		m.currentBlockID, err = m.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("could not append new block: %w", err)
		}
		boundary = m.page.Int32(0)
	}
	recPos := boundary - needBytes
	m.page.SetBytes(recPos, rec)
	m.page.SetInt32(0, recPos)
	m.latestLSN += 1
	return m.latestLSN, nil
}

func (m *Manager) Iterator() func(func([]byte) bool) {
	m.flush()
	p := file.NewPage(m.fm.BlockSize())
	blockID := m.currentBlockID
	var currentPos int32
	moveToBlock := func(blkID file.BlockID) bool {
		err := m.fm.Read(blkID, p)
		if err != nil {
			return false
		}
		currentPos = p.Int32(0) // boundary
		return true
	}
	moveToBlock(blockID)
	return func(yield func([]byte) bool) {
		for {
			if currentPos >= m.fm.BlockSize() && blockID.Num() <= 0 {
				return
			}
			if currentPos == m.fm.BlockSize() {
				blockID = file.NewBlockID(blockID.Filename(), blockID.Num()-1)
				if !moveToBlock(blockID) {
					return
				}
			}
			rec := p.Bytes(currentPos)
			currentPos += 4 + int32(len(rec))
			if !yield(rec) {
				return
			}
		}
	}
}

func (m *Manager) appendNewBlock() (file.BlockID, error) {
	blockID, err := m.fm.Append(m.filename)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("could not append: %w", err)
	}
	m.page.SetInt32(0, m.fm.BlockSize())
	if err := m.fm.Write(blockID, m.page); err != nil {
		return file.BlockID{}, fmt.Errorf("could not write: %w", err)
	}
	return blockID, nil
}

func (m *Manager) flush() error {
	if err := m.fm.Write(m.currentBlockID, m.page); err != nil {
		return fmt.Errorf("could not write: %w", err)
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}
