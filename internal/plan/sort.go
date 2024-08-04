package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
)

type SortPlan struct {
	tx         *transaction.Transaction
	p          Plan
	sche       schema.Schema
	sortFields []schema.FieldName
	order      query.Order
}

var _ Plan = (*SortPlan)(nil)

func NewSortPlan(tx *transaction.Transaction, p Plan, order query.Order) *SortPlan {
	sortFields := make([]schema.FieldName, 0)
	for _, el := range order {
		sortFields = append(sortFields, el.Field)
	}
	return &SortPlan{tx: tx, p: p, sche: *p.Schema(), sortFields: sortFields, order: order}
}

func (s SortPlan) Result() {}

func (s SortPlan) Info() Info {
	conditions := make(map[string][]string)
	for _, fld := range s.sortFields {
		conditions["sortFields"] = append(conditions["sortFields"], string(fld))
	}
	return Info{
		NodeType:      "Sort",
		Conditions:    conditions,
		BlockAccessed: s.BlockAccessed(),
		RecordsOutput: s.RecordsOutput(),
		Children:      []Info{s.p.Info()},
	}
}

func (s SortPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return s.p.Placeholders(findSchema)
}

func (s SortPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	bound, err := s.p.SwapParams(params)
	if err != nil {
		return nil, err
	}
	bp, ok := bound.(*BoundPlan)
	if !ok {
		return nil, fmt.Errorf("bound.(*plan.BoundPlan) error")
	}
	return &BoundPlan{
		Plan: NewSortPlan(s.tx, bp, s.order),
	}, nil
}

func (s SortPlan) Open() (query.Scan, error) {
	src, err := s.p.Open()
	if err != nil {
		return nil, fmt.Errorf("p.Open error: %w", err)
	}

	// split the source into sorted runs
	var runs []TempTable
	if err := src.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("src.BeforeFirst error: %w", err)
	}
	ok, err := src.Next()
	if err != nil {
		return nil, fmt.Errorf("src.Next error: %w", err)
	}
	if ok {
		currentTemp := NewTempTable(s.tx, s.sche)
		runs = append(runs, *currentTemp)
		currentScan, err := currentTemp.Open()
		if err != nil {
			return nil, fmt.Errorf("currentTemp.Open error: %w", err)
		}
		for {
			ok, err := s.copy(src, currentScan)
			if err != nil {
				return nil, fmt.Errorf("copy error: %w", err)
			}
			if !ok {
				break
			}
			cmpRes, err := s.order.Compare(src, currentScan)
			if err != nil {
				return nil, fmt.Errorf("NewOrder.Compare error: %w", err)
			}
			if cmpRes < 0 {
				if err := currentScan.Close(); err != nil {
					return nil, fmt.Errorf("currentScan.Close error: %w", err)
				}
				currentTemp = NewTempTable(s.tx, s.sche)
				runs = append(runs, *currentTemp)
				currentScan, err = currentTemp.Open()
				if err != nil {
					return nil, fmt.Errorf("currentTemp.Open error: %w", err)
				}
			}
		}
		if err := currentScan.Close(); err != nil {
			return nil, fmt.Errorf("currentScan.Close error: %w", err)
		}
	}
	if err := src.Close(); err != nil {
		return nil, fmt.Errorf("src.Close error: %w", err)
	}
	for {
		if len(runs) <= 2 {
			break
		}
		// do a merge iteration
		newRuns := make([]TempTable, 0)
		for len(runs) > 1 {
			p1 := runs[0]
			p2 := runs[1]
			runs = runs[2:]
			// merge two runs
			src1, err := p1.Open()
			if err != nil {
				return nil, fmt.Errorf("p1.Open error: %w", err)
			}
			src2, err := p2.Open()
			if err != nil {
				return nil, fmt.Errorf("p2.Open error: %w", err)
			}
			res := NewTempTable(s.tx, s.sche)
			dest, err := res.Open()
			if err != nil {
				return nil, fmt.Errorf("res.Open error: %w", err)
			}
			ok1, err := src1.Next()
			if err != nil {
				return nil, fmt.Errorf("src1.Next error: %w", err)
			}
			ok2, err := src2.Next()
			if err != nil {
				return nil, fmt.Errorf("src2.Next error: %w", err)
			}
			for ok1 && ok2 {
				cmpRes, err := s.order.Compare(src1, src2)
				if err != nil {
					return nil, fmt.Errorf("NewOrder.Compare error: %w", err)
				}
				if cmpRes < 0 {
					ok1, err = s.copy(src1, dest)
					if err != nil {
						return nil, fmt.Errorf("copy error: %w", err)
					}
				} else {
					ok2, err = s.copy(src2, dest)
					if err != nil {
						return nil, fmt.Errorf("copy error: %w", err)
					}
				}
			}
			if ok1 {
				for ok1 {
					ok1, err = s.copy(src1, dest)
					if err != nil {
						return nil, fmt.Errorf("copy error: %w", err)
					}
				}
			} else {
				for ok2 {
					ok2, err = s.copy(src2, dest)
					if err != nil {
						return nil, fmt.Errorf("copy error: %w", err)
					}
				}
			}
			if err := src1.Close(); err != nil {
				return nil, fmt.Errorf("src1.Close error: %w", err)
			}
			if err := src2.Close(); err != nil {
				return nil, fmt.Errorf("src2.Close error: %w", err)
			}
			if err := dest.Close(); err != nil {
				return nil, fmt.Errorf("dest.Close error: %w", err)
			}
			newRuns = append(newRuns, *res)
		}
		if len(runs) == 1 {
			newRuns = append(newRuns, runs[0])
		}
		runs = newRuns
	}
	newScan, err := NewSortScan(runs, s.order)
	if err != nil {
		return nil, fmt.Errorf("NewSortScan error: %w", err)
	}
	return newScan, nil
}

func (s SortPlan) BlockAccessed() int {
	return NewMaterializePlan(s.tx, s.p).BlockAccessed()
}

func (s SortPlan) RecordsOutput() int {
	return s.p.RecordsOutput()
}

func (s SortPlan) DistinctValues(fieldName schema.FieldName) int {
	return s.p.DistinctValues(fieldName)
}

func (s SortPlan) Schema() *schema.Schema {
	return &s.sche
}

func (s SortPlan) copy(src query.Scan, dest query.UpdateScan) (bool, error) {
	if err := dest.Insert(); err != nil {
		return false, fmt.Errorf("dest.Insert error: %w", err)
	}
	for _, fldName := range s.sche.FieldNames() {
		val, err := src.Val(fldName)
		if err != nil {
			return false, fmt.Errorf("src.Val error: %w", err)
		}
		if err := dest.SetVal(fldName, val); err != nil {
			return false, fmt.Errorf("dest.SetVal error: %w", err)
		}
	}
	ok, err := src.Next()
	if err != nil {
		return false, fmt.Errorf("src.Next error: %w", err)
	}
	return ok, nil
}

type SortScan struct {
	s1, s2, currentScan            query.UpdateScan
	order                          query.Order
	hasMore1, hasMore2             bool
	savedPosition1, savedPosition2 *schema.RID
}

var _ query.Scan = (*SortScan)(nil)

func NewSortScan(runs []TempTable, order query.Order) (*SortScan, error) {
	if len(runs) == 0 || len(runs) > 2 {
		return nil, fmt.Errorf("runs length error")
	}
	s1, err := runs[0].Open()
	if err != nil {
		return nil, fmt.Errorf("runs[0].Open error: %w", err)
	}
	var s2 query.UpdateScan
	if len(runs) == 2 {
		s2, err = runs[1].Open()
		if err != nil {
			return nil, fmt.Errorf("runs[1].Open error: %w", err)
		}
	}
	hasMore1, err := s1.Next()
	if err != nil {
		return nil, fmt.Errorf("s1.Next error: %w", err)
	}
	var hasMore2 bool
	if s2 != nil {
		ok, err := s2.Next()
		if err != nil {
			return nil, fmt.Errorf("s2.Next error: %w", err)
		}
		hasMore2 = ok
	}
	return &SortScan{
		s1:       s1,
		s2:       s2,
		order:    order,
		hasMore1: hasMore1,
		hasMore2: hasMore2,
	}, nil
}

func (s *SortScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	if s.currentScan == nil {
		return nil, fmt.Errorf("currentScan is nil")
	}
	return s.currentScan.Val(fieldName)
}

func (s *SortScan) BeforeFirst() error {
	s.currentScan = nil
	if err := s.s1.BeforeFirst(); err != nil {
		return fmt.Errorf("s1.BeforeFirst error: %w", err)
	}
	hasMore1, err := s.s1.Next()
	if err != nil {
		return fmt.Errorf("s1.Next error: %w", err)
	}
	s.hasMore1 = hasMore1
	if s.s2 != nil {
		if err := s.s2.BeforeFirst(); err != nil {
			return fmt.Errorf("s2.BeforeFirst error: %w", err)
		}
		hasMore2, err := s.s2.Next()
		if err != nil {
			return fmt.Errorf("s2.Next error: %w", err)
		}
		s.hasMore2 = hasMore2
	}
	return nil
}

func (s *SortScan) Next() (bool, error) {
	if s.currentScan != nil {
		if s.currentScan == s.s1 {
			hasMore1, err := s.s1.Next()
			if err != nil {
				return false, fmt.Errorf("s1.Next error: %w", err)
			}
			s.hasMore1 = hasMore1
		} else if s.currentScan == s.s2 {
			hasMore2, err := s.s2.Next()
			if err != nil {
				return false, fmt.Errorf("s2.Next error: %w", err)
			}
			s.hasMore2 = hasMore2
		}
	}
	if !s.hasMore1 && !s.hasMore2 {
		return false, nil
	} else if s.hasMore1 && s.hasMore2 {
		cmpRes, err := s.order.Compare(s.s1, s.s2)
		if err != nil {
			return false, fmt.Errorf("comparator.Compare error: %w", err)
		}
		if cmpRes < 0 {
			s.currentScan = s.s1
		} else {
			s.currentScan = s.s2
		}
	} else if s.hasMore1 {
		s.currentScan = s.s1
	} else {
		s.currentScan = s.s2
	}
	return true, nil
}

func (s *SortScan) Int32(fieldName schema.FieldName) (int32, error) {
	if s.currentScan == nil {
		return 0, fmt.Errorf("currentScan is nil")
	}
	v, err := s.currentScan.Int32(fieldName)
	if err != nil {
		return 0, fmt.Errorf("currentScan.Int32 error: %w", err)
	}
	return v, nil
}

func (s *SortScan) Str(fieldName schema.FieldName) (string, error) {
	if s.currentScan == nil {
		return "", fmt.Errorf("currentScan is nil")
	}
	v, err := s.currentScan.Str(fieldName)
	if err != nil {
		return "", fmt.Errorf("currentScan.Str error: %w", err)
	}
	return v, nil
}

func (s *SortScan) HasField(fieldName schema.FieldName) bool {
	if s.currentScan == nil {
		return false
	}
	return s.currentScan.HasField(fieldName)
}

func (s *SortScan) Close() error {
	if s.s1 != nil {
		if err := s.s1.Close(); err != nil {
			return fmt.Errorf("s1.Close error: %w", err)
		}
	}
	if s.s2 != nil {
		if err := s.s2.Close(); err != nil {
			return fmt.Errorf("s2.Close error: %w", err)
		}
	}
	return nil
}

func (s *SortScan) SavePosition() {
	rid1 := s.s1.RID()
	s.savedPosition1 = &rid1
	if s.s2 != nil {
		rid2 := s.s2.RID()
		s.savedPosition2 = &rid2
	}
}

func (s *SortScan) RestorePosition() error {
	if s.savedPosition1 != nil {
		if err := s.s1.MoveToRID(*s.savedPosition1); err != nil {
			return fmt.Errorf("s1.MoveToRID error: %w", err)
		}
	}
	if s.savedPosition2 != nil {
		if err := s.s2.MoveToRID(*s.savedPosition2); err != nil {
			return fmt.Errorf("s2.MoveToRID error: %w", err)
		}
	}
	return nil
}
