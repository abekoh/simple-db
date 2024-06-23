package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Plan interface {
	Open() (query.Scan, error)
	BlockAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName schema.FieldName) int
	Schema() *schema.Schema
}

type TablePlan struct {
	tableName string
	tx        *transaction.Transaction
	layout    *record.Layout
	statInfo  *metadata.StatInfo
}

var _ Plan = (*TablePlan)(nil)

func NewTablePlan(
	tableName string,
	tx *transaction.Transaction,
	layout *record.Layout,
	statInfo *metadata.StatInfo) *TablePlan {
	return &TablePlan{
		tableName: tableName,
		tx:        tx,
		layout:    layout,
		statInfo:  statInfo,
	}
}

func (t TablePlan) Open() (query.Scan, error) {
	return record.NewTableScan(t.tx, t.tableName, t.layout)
}

func (t TablePlan) BlockAccessed() int {
	return t.statInfo.BlocksAccessed()
}

func (t TablePlan) RecordsOutput() int {
	return t.statInfo.RecordsOutput()
}

func (t TablePlan) DistinctValues(fieldName schema.FieldName) int {
	return t.statInfo.DistinctValues(fieldName)
}

func (t TablePlan) Schema() *schema.Schema {
	return t.layout.Schema()
}

type ProductPlan struct {
	p1, p2 Plan
	sche   schema.Schema
}

var _ Plan = (*ProductPlan)(nil)

func NewProductPlan(p1, p2 Plan) (*ProductPlan, error) {
	s := schema.NewSchema()
	s.AddAll(*p1.Schema())
	s.AddAll(*p2.Schema())
	return &ProductPlan{p1: p1, p2: p2, sche: s}, nil
}

func (p ProductPlan) Open() (query.Scan, error) {
	s1, err := p.p1.Open()
	if err != nil {
		return nil, fmt.Errorf("p1.Open error: %w", err)
	}
	s2, err := p.p2.Open()
	if err != nil {
		return nil, fmt.Errorf("p2.Open error: %w", err)
	}
	return query.NewProductScan(s1, s2)
}

func (p ProductPlan) BlockAccessed() int {
	return p.p1.BlockAccessed() + (p.p1.RecordsOutput() * p.p2.BlockAccessed())
}

func (p ProductPlan) RecordsOutput() int {
	return p.p1.RecordsOutput() * p.p2.RecordsOutput()
}

func (p ProductPlan) DistinctValues(fieldName schema.FieldName) int {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	} else {
		return p.p2.DistinctValues(fieldName)
	}
}

func (p ProductPlan) Schema() *schema.Schema {
	return &p.sche
}

type SelectPlan struct {
	p    Plan
	pred query.Predicate
}

var _ Plan = (*SelectPlan)(nil)

func NewSelectPlan(p Plan, pred query.Predicate) *SelectPlan {
	return &SelectPlan{p: p, pred: pred}
}

func (s SelectPlan) Open() (query.Scan, error) {
	scan, err := s.p.Open()
	if err != nil {
		return nil, fmt.Errorf("p.Open error: %w", err)
	}
	return query.NewSelectScan(scan, s.pred), nil
}

func (s SelectPlan) BlockAccessed() int {
	return s.p.BlockAccessed()
}

func (s SelectPlan) RecordsOutput() int {
	f := 1
	for _, t := range s.pred {
		f *= t.ReductionFactor(s.p.DistinctValues)
	}
	return s.p.RecordsOutput() / f
}

func (s SelectPlan) DistinctValues(fieldName schema.FieldName) int {
	if _, ok := s.pred.EquatesWithConstant(fieldName); !ok {
		return 1
	} else {
		fieldName2, ok := s.pred.EquatesWithField(fieldName)
		if ok {
			return min(s.p.DistinctValues(fieldName), s.p.DistinctValues(fieldName2))
		} else {
			return s.p.DistinctValues(fieldName)
		}
	}
}

func (s SelectPlan) Schema() *schema.Schema {
	return s.p.Schema()
}
