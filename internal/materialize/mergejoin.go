package materialize

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
)

type MergeJoinScan struct {
	s1                     query.Scan
	s2                     SortScan
	fieldName1, fieldName2 schema.FieldName
	joinValue              schema.Constant
}

var _ query.Scan = (*MergeJoinScan)(nil)

func NewMergeJoinScan(s1 query.Scan, s2 SortScan, fieldName1, fieldName2 schema.FieldName) (*MergeJoinScan, error) {
	ms := MergeJoinScan{s1: s1, s2: s2, fieldName1: fieldName1, fieldName2: fieldName2}
	return &ms, nil
}

func (m MergeJoinScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	if m.s1.HasField(fieldName) {
		v, err := m.s1.Val(fieldName)
		if err != nil {
			return nil, fmt.Errorf("s1.Val error: %w", err)
		}
		return v, nil
	} else {
		v, err := m.s2.Val(fieldName)
		if err != nil {
			return nil, fmt.Errorf("s2.Val error: %w", err)
		}
		return v, nil
	}
}

func (m MergeJoinScan) BeforeFirst() error {
	if err := m.s1.BeforeFirst(); err != nil {
		return fmt.Errorf("s1.BeforeFirst error: %w", err)
	}
	if err := m.s2.BeforeFirst(); err != nil {
		return fmt.Errorf("s2.BeforeFirst error: %w", err)
	}
	return nil
}

func (m MergeJoinScan) Next() (bool, error) {
	ok2, err := m.s2.Next()
	if err != nil {
		return false, fmt.Errorf("s2.Next error: %w", err)
	}
	if ok2 {
		v2, err := m.s2.Val(m.fieldName2)
		if err != nil {
			return false, fmt.Errorf("s2.Val error: %w", err)
		}
		if m.joinValue != nil && v2.Equals(m.joinValue) {
			return true, nil
		}
		return true, nil
	}
	ok1, err := m.s1.Next()
	if err != nil {
		return false, fmt.Errorf("s1.Next error: %w", err)
	}
	if ok1 {
		v1, err := m.s1.Val(m.fieldName1)
		if err != nil {
			return false, fmt.Errorf("s1.Val error: %w", err)
		}
		if m.joinValue != nil && v1.Equals(m.joinValue) {
			if err := m.s2.RestorePosition(); err != nil {
				return false, fmt.Errorf("s2.RestorePosition error: %w", err)
			}
			return true, nil
		}
	}
	for ok1 && ok2 {
		v1, err := m.s1.Val(m.fieldName1)
		if err != nil {
			return false, fmt.Errorf("s1.Val error: %w", err)
		}
		v2, err := m.s2.Val(m.fieldName2)
		if err != nil {
			return false, fmt.Errorf("s2.Val error: %w", err)
		}
		if v1.Compare(v2) < 0 {
			ok1, err = m.s1.Next()
			if err != nil {
				return false, fmt.Errorf("s1.Next error: %w", err)
			}
		} else if v1.Compare(v2) > 0 {
			ok2, err = m.s2.Next()
			if err != nil {
				return false, fmt.Errorf("s2.Next error: %w", err)
			}
		} else {
			m.s2.SavePosition()
			jv, err := m.s2.Val(m.fieldName2)
			if err != nil {
				return false, fmt.Errorf("s2.Val error: %w", err)
			}
			m.joinValue = jv
			return true, nil
		}
	}
	return false, nil
}

func (m MergeJoinScan) Int32(fieldName schema.FieldName) (int32, error) {
	if m.s1.HasField(fieldName) {
		v, err := m.s1.Int32(fieldName)
		if err != nil {
			return 0, fmt.Errorf("s1.Int32 error: %w", err)
		}
		return v, nil
	} else {
		v, err := m.s2.Int32(fieldName)
		if err != nil {
			return 0, fmt.Errorf("s2.Int32 error: %w", err)
		}
		return v, nil
	}
}

func (m MergeJoinScan) Str(fieldName schema.FieldName) (string, error) {
	if m.s1.HasField(fieldName) {
		v, err := m.s1.Str(fieldName)
		if err != nil {
			return "", fmt.Errorf("s1.Str error: %w", err)
		}
		return v, nil
	} else {
		v, err := m.s2.Str(fieldName)
		if err != nil {
			return "", fmt.Errorf("s2.Str error: %w", err)
		}
		return v, nil
	}
}

func (m MergeJoinScan) HasField(fieldName schema.FieldName) bool {
	return m.s1.HasField(fieldName) || m.s2.HasField(fieldName)
}

func (m MergeJoinScan) Close() error {
	if err := m.s1.Close(); err != nil {
		return fmt.Errorf("s1.Close error: %w", err)
	}
	if err := m.s2.Close(); err != nil {
		return fmt.Errorf("s2.Close error: %w", err)
	}
	return nil
}
