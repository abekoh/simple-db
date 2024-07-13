package index

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/query"
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
	Initializer = func(tx *transaction.Transaction, idxName string, layout *record.Layout) Index
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

func NewHashIndex(tx *transaction.Transaction, idxName string, layout *record.Layout) Index {
	return &HashIndex{tx: tx, idxName: idxName, layout: layout}
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

type SelectScan struct {
	tableScan *record.TableScan
	idx       Index
	val       schema.Constant
}

func NewSelectScan(tableScan *record.TableScan, idx Index, val schema.Constant) (*SelectScan, error) {
	s := &SelectScan{tableScan: tableScan, idx: idx, val: val}
	if err := s.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("BeforeFirst error: %w", err)
	}
	return s, nil
}

var _ query.Scan = (*SelectScan)(nil)

func (s SelectScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	val, err := s.tableScan.Val(fieldName)
	if err != nil {
		return nil, fmt.Errorf("tableScan.Val error: %w", err)
	}
	return val, nil
}

func (s SelectScan) BeforeFirst() error {
	if err := s.idx.BeforeFirst(s.val); err != nil {
		return fmt.Errorf("index.BeforeFirst error: %w", err)
	}
	return nil
}

func (s SelectScan) Next() (bool, error) {
	ok, err := s.idx.Next()
	if err != nil {
		return false, fmt.Errorf("index.Next error: %w", err)
	}
	if ok {
		rid, err := s.idx.DataRID()
		if err != nil {
			return false, fmt.Errorf("index.DataRID error: %w", err)
		}
		if err := s.tableScan.MoveToRID(rid); err != nil {
			return false, fmt.Errorf("tableScan.MoveToRID error: %w", err)
		}
	}
	return ok, nil
}

func (s SelectScan) Int32(fieldName schema.FieldName) (int32, error) {
	val, err := s.tableScan.Int32(fieldName)
	if err != nil {
		return 0, fmt.Errorf("tableScan.Int32 error: %w", err)
	}
	return val, nil
}

func (s SelectScan) Str(fieldName schema.FieldName) (string, error) {
	val, err := s.tableScan.Str(fieldName)
	if err != nil {
		return "", fmt.Errorf("tableScan.Str error: %w", err)
	}
	return val, nil
}

func (s SelectScan) HasField(fieldName schema.FieldName) bool {
	return s.tableScan.HasField(fieldName)
}

func (s SelectScan) Close() error {
	if err := s.idx.Close(); err != nil {
		return fmt.Errorf("index.Close error: %w", err)
	}
	if err := s.tableScan.Close(); err != nil {
		return fmt.Errorf("tableScan.Close error: %w", err)
	}
	return nil
}

type JoinScan struct {
	lhs       query.Scan
	rhs       *record.TableScan
	idx       Index
	joinField schema.FieldName
}

func NewJoinScan(lhs query.Scan, idx Index, joinField schema.FieldName, rh *record.TableScan) (*JoinScan, error) {
	js := &JoinScan{lhs: lhs, idx: idx, joinField: joinField, rhs: rh}
	if err := js.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("BeforeFirst error: %w", err)
	}
	return js, nil
}

var _ query.Scan = (*JoinScan)(nil)

func (j JoinScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	if j.lhs.HasField(fieldName) {
		if val, err := j.lhs.Val(fieldName); err != nil {
			return nil, fmt.Errorf("lhs.Val error: %w", err)
		} else {
			return val, nil
		}
	} else {
		if val, err := j.rhs.Val(fieldName); err != nil {
			return nil, fmt.Errorf("rhs.Val error: %w", err)
		} else {
			return val, nil
		}
	}
}

func (j JoinScan) BeforeFirst() error {
	if err := j.lhs.BeforeFirst(); err != nil {
		return fmt.Errorf("lhs.BeforeFirst error: %w", err)
	}
	if _, err := j.lhs.Next(); err != nil {
		return fmt.Errorf("lhs.Next error: %w", err)
	}
	if err := j.resetIndex(); err != nil {
		return fmt.Errorf("resetIndex error: %w", err)
	}
	return nil
}

func (j JoinScan) Next() (bool, error) {
	for {
		idxOk, err := j.idx.Next()
		if err != nil {
			return false, fmt.Errorf("index.Next error: %w", err)
		}
		if idxOk {
			rid, err := j.idx.DataRID()
			if err != nil {
				return false, fmt.Errorf("index.DataRID error: %w", err)
			}
			if err := j.rhs.MoveToRID(rid); err != nil {
				return false, fmt.Errorf("rhs.MoveToRID error: %w", err)
			}
		}
		lhsOk, err := j.lhs.Next()
		if err != nil {
			return false, fmt.Errorf("lhs.Next error: %w", err)
		}
		if !lhsOk {
			return false, nil
		}
		if err := j.resetIndex(); err != nil {
			return false, fmt.Errorf("resetIndex error: %w", err)
		}
	}
}

func (j JoinScan) Int32(fieldName schema.FieldName) (int32, error) {
	if j.lhs.HasField(fieldName) {
		if val, err := j.lhs.Int32(fieldName); err != nil {
			return 0, fmt.Errorf("lhs.Int32 error: %w", err)
		} else {
			return val, nil
		}
	} else {
		if val, err := j.rhs.Int32(fieldName); err != nil {
			return 0, fmt.Errorf("rhs.Int32 error: %w", err)
		} else {
			return val, nil
		}
	}
}

func (j JoinScan) Str(fieldName schema.FieldName) (string, error) {
	if j.lhs.HasField(fieldName) {
		if val, err := j.lhs.Str(fieldName); err != nil {
			return "", fmt.Errorf("lhs.Str error: %w", err)
		} else {
			return val, nil
		}
	} else {
		if val, err := j.rhs.Str(fieldName); err != nil {
			return "", fmt.Errorf("rhs.Str error: %w", err)
		} else {
			return val, nil
		}
	}
}

func (j JoinScan) HasField(fieldName schema.FieldName) bool {
	return j.lhs.HasField(fieldName) || j.rhs.HasField(fieldName)
}

func (j JoinScan) Close() error {
	if err := j.lhs.Close(); err != nil {
		return fmt.Errorf("lhs.Close error: %w", err)
	}
	if err := j.idx.Close(); err != nil {
		return fmt.Errorf("index.Close error: %w", err)
	}
	if err := j.rhs.Close(); err != nil {
		return fmt.Errorf("rhs.Close error: %w", err)
	}
	return nil
}

func (j JoinScan) resetIndex() error {
	searchKey, err := j.lhs.Val(j.joinField)
	if err != nil {
		return fmt.Errorf("lhs.Val error: %w", err)
	}
	if err := j.idx.BeforeFirst(searchKey); err != nil {
		return fmt.Errorf("index.BeforeFirst error: %w", err)
	}
	return nil
}
