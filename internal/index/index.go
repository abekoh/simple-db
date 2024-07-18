package index

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type (
	Index interface {
		BeforeFirst(searchKey schema.Constant) error
		Next() (bool, error)
		DataRID() (schema.RID, error)
		Insert(dataVal schema.Constant, dataRID schema.RID) error
		Delete(dataVal schema.Constant, dataRID schema.RID) error
		Close() error
	}
	Initializer = func(tx *transaction.Transaction, idxName string, layout *record.Layout) (Index, error)
	SearchCost  = func(numBlocks, rpb int) int
	Config      struct {
		Initializer Initializer
		SearchCost  SearchCost
	}
)

var (
	ConfigHash = &Config{
		Initializer: NewHashIndex,
		SearchCost:  HashSearchCost,
	}
)

const (
	blockFld = "block"
	idFld    = "id"
	dataFld  = "dataval"
)

func NewIndexLayout(field schema.Field) *record.Layout {
	sche := schema.NewSchema()
	sche.AddInt32Field(blockFld)
	sche.AddInt32Field(idFld)
	sche.AddField(dataFld, field)
	return record.NewLayoutSchema(sche)
}

const numBuckets = 100

type HashIndex struct {
	tx        *transaction.Transaction
	idxName   string
	layout    *record.Layout
	searchKey schema.Constant
	tableScan *record.TableScan
}

func NewHashIndex(tx *transaction.Transaction, idxName string, layout *record.Layout) (Index, error) {
	return &HashIndex{tx: tx, idxName: idxName, layout: layout}, nil
}

func HashSearchCost(numBlocks, rpb int) int {
	return numBlocks / numBuckets
}

var _ Index = (*HashIndex)(nil)

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
		val, err := h.tableScan.Val(dataFld)
		if err != nil {
			return false, fmt.Errorf("tableScan.Val error: %w", err)
		}
		if h.searchKey.Equals(val) {
			return true, nil
		}
	}
}

func (h *HashIndex) DataRID() (schema.RID, error) {
	blockNum, err := h.tableScan.Int32(blockFld)
	if err != nil {
		return schema.RID{}, fmt.Errorf("tableScan.Int32 error: %w", err)
	}
	id, err := h.tableScan.Int32(idFld)
	if err != nil {
		return schema.RID{}, fmt.Errorf("tableScan.Int32 error: %w", err)
	}
	return schema.NewRID(blockNum, id), nil
}

func (h *HashIndex) Insert(dataVal schema.Constant, dataRID schema.RID) error {
	if err := h.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("index.BeforeFirst error: %w", err)
	}
	if err := h.tableScan.Insert(); err != nil {
		return fmt.Errorf("index.Insert error: %w", err)
	}
	if err := h.tableScan.SetInt32(blockFld, dataRID.BlockNum()); err != nil {
		return fmt.Errorf("tableScan.SetInt32 error: %w", err)
	}
	if err := h.tableScan.SetInt32(idFld, dataRID.Slot()); err != nil {
		return fmt.Errorf("tableScan.SetInt32 error: %w", err)
	}
	if err := h.tableScan.SetVal(dataFld, dataVal); err != nil {
		return fmt.Errorf("tableScan.SetVal error: %w", err)
	}
	return nil
}

func (h *HashIndex) Delete(dataVal schema.Constant, dataRID schema.RID) error {
	if err := h.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("index.BeforeFirst error: %w", err)
	}
	for {
		ok, err := h.Next()
		if err != nil {
			return fmt.Errorf("index.Next error: %w", err)
		}
		if !ok {
			return nil
		}
		rid, err := h.DataRID()
		if err != nil {
			return fmt.Errorf("index.DataRID error: %w", err)
		}
		if rid.Equals(dataRID) {
			if err := h.tableScan.Delete(); err != nil {
				return fmt.Errorf("tableScan.Delete error: %w", err)
			}
			return nil
		}
	}
}

func (h *HashIndex) Close() error {
	if h.tableScan != nil {
		if err := h.tableScan.Close(); err != nil {
			return fmt.Errorf("tableScan.Close error: %w", err)
		}
	}
	return nil
}
