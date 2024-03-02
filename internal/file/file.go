package file

import (
	"fmt"
	"os"
	"path"
	"sync"
)

type BlockID struct {
	filename string
	blkNum   int32
}

func NewBlockID(filename string, blkNum int32) BlockID {
	return BlockID{
		filename: filename,
		blkNum:   blkNum,
	}
}

func (b BlockID) Filename() string {
	return b.filename
}

func (b BlockID) Num() int32 {
	return b.blkNum
}

type Page struct {
	bb []byte
}

func NewPage(blockSize int32) *Page {
	return &Page{
		bb: make([]byte, blockSize),
	}
}

func NewPageBytes(b []byte) *Page {
	return &Page{
		bb: b,
	}
}

func (p *Page) Int32(offset int32) int32 {
	d := p.bb[offset : offset+4]
	return int32(d[3])<<24 | int32(d[2])<<16 | int32(d[1])<<8 | int32(d[0])
}

func (p *Page) SetInt32(offset int32, n int32) {
	p.bb[offset] = byte(n)
	p.bb[offset+1] = byte(n >> 8)
	p.bb[offset+2] = byte(n >> 16)
	p.bb[offset+3] = byte(n >> 24)
}

func (p *Page) Bytes(offset int32) []byte {
	n := p.Int32(offset)
	return p.bb[offset+4 : offset+4+n]
}

func (p *Page) SetBytes(offset int32, b []byte) {
	p.SetInt32(offset, int32(len(b)))
	copy(p.bb[offset+4:offset+4+int32(len(b))], b)
}

func (p *Page) Str(offset int32) string {
	return string(p.Bytes(offset))
}

func (p *Page) SetStr(offset int32, s string) {
	p.SetBytes(offset, []byte(s))
}

type Manager struct {
	dbDirPath string
	blockSize int32
	isNew     bool
	openFiles map[string]*os.File
	kmu       keyedMutex
}

func NewManager(dbDirPath string, blockSize int32) (*Manager, error) {
	isNew := false
	fi, err := os.Stat(dbDirPath)
	if err != nil {
		isNew = true
		if err := os.Mkdir(dbDirPath, 0755); err != nil {
			return nil, err
		}
	} else {
		if !fi.IsDir() {
			return nil, fmt.Errorf("%s is not a directory", dbDirPath)
		}
	}
	return &Manager{
		dbDirPath: dbDirPath,
		blockSize: blockSize,
		isNew:     isNew,
		openFiles: make(map[string]*os.File),
		kmu:       keyedMutex{},
	}, nil
}

func (m *Manager) getFile(filename string) (*os.File, func(), error) {
	unlock := m.kmu.lock(filename)
	if f, ok := m.openFiles[filename]; ok {
		return f, unlock, nil
	}
	f, err := os.OpenFile(path.Join(m.dbDirPath, filename), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, unlock, fmt.Errorf("could not open %s: %w", filename, err)
	}
	m.openFiles[filename] = f
	return f, unlock, nil
}

func (m *Manager) Read(blk BlockID, p *Page) error {
	f, unlock, err := m.getFile(blk.filename)
	defer unlock()
	if err != nil {
		return fmt.Errorf("could not get file: %w", err)
	}
	_, err = f.ReadAt(p.bb, int64(blk.blkNum*m.blockSize))
	if err != nil {
		return fmt.Errorf("could not read at %d: %w", blk.blkNum*m.blockSize, err)
	}
	return nil
}

func (m *Manager) Write(blk BlockID, p *Page) error {
	f, unlock, err := m.getFile(blk.filename)
	defer unlock()
	if err != nil {
		return fmt.Errorf("could not get file: %w", err)
	}
	_, err = f.WriteAt(p.bb, int64(blk.blkNum*m.blockSize))
	if err != nil {
		return fmt.Errorf("could not write at %d: %w", blk.blkNum*m.blockSize, err)
	}
	return nil
}

func (m *Manager) Append(filename string) (BlockID, error) {
	f, unlock, err := m.getFile(filename)
	defer unlock()
	if err != nil {
		return BlockID{}, fmt.Errorf("could not get file: %w", err)
	}
	blkNum, err := m.lengthFromFile(f)
	if err != nil {
		return BlockID{}, fmt.Errorf("could not get lengthFromFile: %w", err)
	}
	blkID := NewBlockID(filename, blkNum)
	b := make([]byte, m.blockSize)
	if _, err := f.Write(b); err != nil {
		return BlockID{}, fmt.Errorf("could not write: %w", err)
	}
	_, err = f.WriteAt(b, int64(blkNum*m.blockSize))
	if err != nil {
		return BlockID{}, fmt.Errorf("could not write at %d: %w", blkNum*m.blockSize, err)
	}
	return blkID, nil
}

func (m *Manager) Length(filename string) (int32, error) {
	f, unlock, err := m.getFile(filename)
	defer unlock()
	if err != nil {
		return 0, fmt.Errorf("could not get file: %w", err)

	}
	return m.lengthFromFile(f)
}

func (m *Manager) lengthFromFile(f *os.File) (int32, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("could not stat file: %w", err)
	}
	return int32(fi.Size() / int64(m.blockSize)), nil
}

func (m *Manager) BlockSize() int32 {
	return m.blockSize
}

type keyedMutex struct {
	mutexes sync.Map
}

func (m *keyedMutex) lock(key string) func() {
	mu, _ := m.mutexes.LoadOrStore(key, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	return func() {
		mu.(*sync.Mutex).Unlock()
	}
}
