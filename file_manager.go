package simpledb

import (
	"bytes"
	"os"
)

type BlockID struct {
	filename string
	blknum   int64
}

func NewBlockID(filename string, blknum int64) BlockID {
	return BlockID{
		filename: filename,
		blknum:   blknum,
	}
}

type Page struct {
	bb        bytes.Buffer
	blocksize int64
}

func NewPageBlocksize(blocksize int64) *Page {
	return &Page{}
}

func NewPageBytes([]byte) *Page {
	return &Page{}
}

func (p *Page) Int(offset int64) int64 {
	return 0
}

func (p *Page) SetInt(offset int64, n int64) error {
	return nil
}

func (p *Page) Bytes(offset int64) []byte {
	return nil
}

func (p *Page) SetBytes(offset int64, b []byte) error {
	return nil
}

func (p *Page) Str(offset int64) string {
	return ""
}

func (p *Page) SetStr(offset int64, s string) error {
	return nil
}

func MaxLength(strlen int) int64 {
	return 0
}

type FileManager struct {
	file      *os.File
	Blocksize int64
	IsNew     bool
	// openFiles map[string]
}

func NewFileManager() *FileManager {
	return &FileManager{}
}

func (m *FileManager) Read(blk BlockID, p *Page) error {
	return nil
}

func (m *FileManager) Write(blk BlockID, p *Page) error {
	return nil
}

func (m *FileManager) Append(filename string) (BlockID, error) {
	return BlockID{}, nil
}

func (m *FileManager) Length(filename string) (int64, error) {
	return 0, nil
}

type SimpleDB struct {
}
