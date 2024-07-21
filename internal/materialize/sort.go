package materialize

import (
	"fmt"
	"strings"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Comparator struct {
	fields []schema.FieldName
}

func NewComparator(fields []schema.FieldName) *Comparator {
	return &Comparator{fields: fields}
}

func (c Comparator) Compare(s1, s2 query.Scan) (int, error) {
	for _, fld := range c.fields {
		val1, err := s1.Val(fld)
		if err != nil {
			return 0, fmt.Errorf("s1.Val error: %w", err)
		}
		val2, err := s2.Val(fld)
		if err != nil {
			return 0, fmt.Errorf("s2.Val error: %w", err)
		}
		cmp := val1.Compare(val2)
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

type SortPlan struct {
	tx         *transaction.Transaction
	p          plan.Plan
	sche       schema.Schema
	sortFields []schema.FieldName
}

var _ plan.Plan = (*SortPlan)(nil)

func NewSortPlan(tx *transaction.Transaction, p plan.Plan, sortFields []schema.FieldName) *SortPlan {
	return &SortPlan{tx: tx, p: p, sche: *p.Schema(), sortFields: sortFields}
}

func (s SortPlan) Result() {}

func (s SortPlan) String() string {
	fields := make([]string, len(s.sortFields))
	for i, fld := range s.sortFields {
		fields[i] = string(fld)
	}
	return fmt.Sprintf("Sort(%s){%s}", strings.Join(fields, ","), s.p)
}

func (s SortPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return s.p.Placeholders(findSchema)
}

func (s SortPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	bound, err := s.p.SwapParams(params)
	if err != nil {
		return nil, err
	}
	bp, ok := bound.(*plan.BoundPlan)
	if !ok {
		return nil, fmt.Errorf("bound.(*plan.BoundPlan) error")
	}
	return &plan.BoundPlan{
		Plan: NewSortPlan(s.tx, bp, s.sortFields),
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
			cmpRes, err := NewComparator(s.sortFields).Compare(src, currentScan)
			if err != nil {
				return nil, fmt.Errorf("NewComparator.Compare error: %w", err)
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
		for len(newRuns) > 1 {
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
				cmpRes, err := NewComparator(s.sortFields).Compare(src1, src2)
				if err != nil {
					return nil, fmt.Errorf("NewComparator.Compare error: %w", err)
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
	newScan, err := NewSortScan(runs, NewComparator(s.sortFields))
	if err != nil {
		return nil, fmt.Errorf("NewSortScan error: %w", err)
	}
	return newScan, nil
}

func (s SortPlan) BlockAccessed() int {
	return NewPlan(s.tx, s.p).BlockAccessed()
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
	s1, s2, currentScan query.UpdateScan
	comparator          *Comparator
	hasMore1, hasMore2  bool
	savedPosition       []schema.RID
}

var _ query.Scan = (*SortScan)(nil)

func NewSortScan(runs []TempTable, comperator *Comparator) (*SortScan, error) {
	s1, err := runs[0].Open()
	if err != nil {
		return nil, fmt.Errorf("runs[0].Open error: %w", err)
	}
	s2, err := runs[1].Open()
	if err != nil {
		return nil, fmt.Errorf("runs[1].Open error: %w", err)
	}
	hasMore1, err := s1.Next()
	if err != nil {
		return nil, fmt.Errorf("s1.Next error: %w", err)
	}
	hasMore2, err := s2.Next()
	if err != nil {
		return nil, fmt.Errorf("s2.Next error: %w", err)
	}
	return &SortScan{
		s1:         s1,
		s2:         s2,
		comparator: comperator,
		hasMore1:   hasMore1,
		hasMore2:   hasMore2,
	}, nil
}

func (s SortScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	//TODO implement me
	panic("implement me")
}

func (s SortScan) BeforeFirst() error {
	//TODO implement me
	panic("implement me")
}

func (s SortScan) Next() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (s SortScan) Int32(fieldName schema.FieldName) (int32, error) {
	//TODO implement me
	panic("implement me")
}

func (s SortScan) Str(fieldName schema.FieldName) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s SortScan) HasField(fieldName schema.FieldName) bool {
	//TODO implement me
	panic("implement me")
}

func (s SortScan) Close() error {
	//TODO implement me
	panic("implement me")
}
