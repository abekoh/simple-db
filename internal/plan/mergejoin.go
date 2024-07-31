package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
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
	if err := ms.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("ms.BeforeFirst error: %w", err)
	}
	return &ms, nil
}

func (m *MergeJoinScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
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

func (m *MergeJoinScan) BeforeFirst() error {
	if err := m.s1.BeforeFirst(); err != nil {
		return fmt.Errorf("s1.BeforeFirst error: %w", err)
	}
	if err := m.s2.BeforeFirst(); err != nil {
		return fmt.Errorf("s2.BeforeFirst error: %w", err)
	}
	return nil
}

func (m *MergeJoinScan) Next() (bool, error) {
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

func (m *MergeJoinScan) Int32(fieldName schema.FieldName) (int32, error) {
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

func (m *MergeJoinScan) Str(fieldName schema.FieldName) (string, error) {
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

func (m *MergeJoinScan) HasField(fieldName schema.FieldName) bool {
	return m.s1.HasField(fieldName) || m.s2.HasField(fieldName)
}

func (m *MergeJoinScan) Close() error {
	if err := m.s1.Close(); err != nil {
		return fmt.Errorf("s1.Close error: %w", err)
	}
	if err := m.s2.Close(); err != nil {
		return fmt.Errorf("s2.Close error: %w", err)
	}
	return nil
}

type MergeJoinPlan struct {
	tx                     *transaction.Transaction
	p1, p2                 Plan
	fieldName1, fieldName2 schema.FieldName
	sche                   schema.Schema
}

var _ Plan = (*MergeJoinPlan)(nil)

func NewMergeJoinPlan(tx *transaction.Transaction, p1, p2 Plan, fieldName1, fieldName2 schema.FieldName) (*MergeJoinPlan, error) {
	sp1 := NewSortPlan(tx, p1, []schema.FieldName{fieldName1})
	sp2 := NewSortPlan(tx, p2, []schema.FieldName{fieldName2})
	sche := schema.NewSchema()
	sche.AddAll(*p1.Schema())
	sche.AddAll(*p2.Schema())
	return &MergeJoinPlan{tx: tx, p1: sp1, p2: sp2, fieldName1: fieldName1, fieldName2: fieldName2, sche: sche}, nil
}

func (m MergeJoinPlan) Result() {}

func (m MergeJoinPlan) String() string {
	return fmt.Sprintf("MergeJoin(%s,%s){%s,%s}", m.fieldName1, m.fieldName2, m.p1, m.p2)
}

func (m MergeJoinPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	placeholders := m.p1.Placeholders(findSchema)
	for k, v := range m.p2.Placeholders(findSchema) {
		placeholders[k] = v
	}
	return placeholders
}

func (m MergeJoinPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	newP1, err := m.p1.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p1.SwapParams error: %w", err)
	}
	newBP1, ok := newP1.(*BoundPlan)
	if !ok {
		return nil, fmt.Errorf("newP1 is not a plan.BoundPlan")
	}
	newP2, err := m.p2.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p2.SwapParams error: %w", err)
	}
	newBP2, ok := newP2.(*BoundPlan)
	if !ok {
		return nil, fmt.Errorf("newP2 is not a plan.BoundPlan")
	}
	newMergeJoinPlan, err := NewMergeJoinPlan(m.tx, newBP1, newBP2, m.fieldName1, m.fieldName2)
	if err != nil {
		return nil, fmt.Errorf("NewMergeJoinPlan error: %w", err)
	}
	return &BoundPlan{
		Plan: newMergeJoinPlan,
	}, nil
}

func (m MergeJoinPlan) Open() (query.Scan, error) {
	s1, err := m.p1.Open()
	if err != nil {
		return nil, fmt.Errorf("p1.Open error: %w", err)
	}
	s2, err := m.p2.Open()
	if err != nil {
		return nil, fmt.Errorf("p2.Open error: %w", err)
	}
	sortScane, ok := s2.(*SortScan)
	if !ok {
		return nil, fmt.Errorf("s2 is not a SortScan")
	}
	ms, err := NewMergeJoinScan(s1, *sortScane, m.fieldName1, m.fieldName2)
	if err != nil {
		return nil, fmt.Errorf("NewMergeJoinScan error: %w", err)
	}
	return ms, nil
}

func (m MergeJoinPlan) BlockAccessed() int {
	return m.p1.BlockAccessed() + m.p2.BlockAccessed()
}

func (m MergeJoinPlan) RecordsOutput() int {
	return (m.p1.RecordsOutput() + m.p2.RecordsOutput()) / max(m.p1.DistinctValues(m.fieldName1), m.p2.DistinctValues(m.fieldName2))
}

func (m MergeJoinPlan) DistinctValues(fieldName schema.FieldName) int {
	if m.p1.Schema().HasField(fieldName) {
		return m.p1.DistinctValues(fieldName)
	}
	return m.p2.DistinctValues(fieldName)
}

func (m MergeJoinPlan) Schema() *schema.Schema {
	return &m.sche
}
