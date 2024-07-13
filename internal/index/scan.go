package index

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
)

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

func NewJoinScan(lhs query.Scan, rhs *record.TableScan, idx Index, joinField schema.FieldName) (*JoinScan, error) {
	js := &JoinScan{lhs: lhs, idx: idx, joinField: joinField, rhs: rhs}
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
			return true, nil
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
