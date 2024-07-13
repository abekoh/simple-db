package index

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Index interface {
	BeforeFirst(searchKey schema.Constant) error
	Next() (bool, error)
	DataRID() (schema.RID, error)
	Insert(searchKey string, dataRID schema.RID) error
	Delete(searchKey string, dataRID schema.RID) error
	Close() error
}

const numBuckets = 100

type HashIndex struct {
	tx        *transaction.Transaction
	idxName   string
	layout    *record.Layout
	searchKey schema.Constant
	tableScan *record.TableScan
}

func NewHashIndex(tx *transaction.Transaction, idxName string, layout *record.Layout) *HashIndex {
	return &HashIndex{tx: tx, idxName: idxName, layout: layout}
}

func (h *HashIndex) BeforeFirst(searchKey schema.Constant) error {
	if err := h.Close(); err != nil {
		return fmt.Errorf("index.Close error: %w", err)
	}
	h.searchKey = searchKey
	bucket := searchKey.HashCode() % numBuckets
	tableName := fmt.Sprintf("%s%d", h.idxName, bucket)
	ts, err := record.NewTableScan(h.tx, tableName, h.layout)
	if err != nil {
		return fmt.Errorf("record.NewTableScan error: %w", err)
	}
	h.tableScan = ts
	return nil
}

func (h *HashIndex) Next() (bool, error) {
	for {
		ok, err := h.tableScan.Next()
		if err != nil {
			return false, fmt.Errorf("tableScan.Next error: %w", err)
		}
		if !ok {
			return false, nil
		}
		val, err := h.tableScan.Val("dataval")
		if err != nil {
			return false, fmt.Errorf("tableScan.Val error: %w", err)
		}
		if h.searchKey.Equals(val) {
			return true, nil
		}
	}
}

func (h *HashIndex) DataRID() (schema.RID, error) {
	//TODO implement me
	panic("implement me")
}

func (h *HashIndex) Insert(searchKey string, dataRID schema.RID) error {
	//TODO implement me
	panic("implement me")
}

func (h *HashIndex) Delete(searchKey string, dataRID schema.RID) error {
	//TODO implement me
	panic("implement me")
}

func (h *HashIndex) Close() error {
	if h.tableScan != nil {
		if err := h.tableScan.Close(); err != nil {
			return fmt.Errorf("tableScan.Close error: %w", err)
		}
	}
	return nil
}
