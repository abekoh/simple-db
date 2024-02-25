package file

import (
	"fmt"
	"os"
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

type Page struct {
	bb []byte
}

func NewPageBlocksize(blockSize int32) *Page {
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
	return p.bb[offset : offset+4]
}

func (p *Page) SetBytes(offset int32, b []byte) {
	copy(p.bb[offset:offset+4], b)
}

func (p *Page) Str(offset int32) string {
	return string(p.bb[offset : offset+4])
}

func (p *Page) SetStr(offset int32, s string) {
	copy(p.bb[offset:offset+4], s)
}

type FileManager struct {
	dbDirPath string
	blockSize int32
	isNew     bool
	openFiles map[string]*os.File
}

func NewFileManager(dbDirPath string, blockSize int32) (*FileManager, error) {
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
	return &FileManager{
		dbDirPath: dbDirPath,
		blockSize: blockSize,
		isNew:     isNew,
	}, nil
}

func (m *FileManager) getFile(filename string) (*os.File, error) {
	if f, ok := m.openFiles[filename]; ok {
		return f, nil
	}
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not open %s: %w", filename, err)
	}
	m.openFiles[filename] = f
	return f, nil
}

func (m *FileManager) Read(blk BlockID, p *Page) error {
	f, err := m.getFile(blk.filename)
	if err != nil {
		return fmt.Errorf("could not get file: %w", err)
	}
	_, err = f.ReadAt(p.bb, int64(blk.blkNum*m.blockSize))
	if err != nil {
		return fmt.Errorf("could not read at %d: %w", blk.blkNum*m.blockSize, err)
	}
	return nil
}

func (m *FileManager) Write(blk BlockID, p *Page) error {
	f, err := m.getFile(blk.filename)
	if err != nil {
		return fmt.Errorf("could not get file: %w", err)
	}
	_, err = f.WriteAt(p.bb, int64(blk.blkNum*m.blockSize))
	if err != nil {
		return fmt.Errorf("could not write at %d: %w", blk.blkNum*m.blockSize, err)
	}
	return nil
}

func (m *FileManager) Append(filename string) (BlockID, error) {
	f, err := m.getFile(filename)
	if err != nil {
		return BlockID{}, fmt.Errorf("could not get file: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		return BlockID{}, fmt.Errorf("could not stat file: %w", err)
	}
	blkNum := int32(fi.Size() / int64(m.blockSize))
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

func (m *FileManager) Length(filename string) (int64, error) {
	return 0, nil
}
