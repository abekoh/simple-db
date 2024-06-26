package plan

import (
	"fmt"
	"strings"

	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Result interface {
	Result()
}

type CommandResult int

func (CommandResult) Result() {}

type Plan interface {
	Result
	fmt.Stringer
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
	statInfo  metadata.StatInfo
}

var _ Plan = (*TablePlan)(nil)

func NewTablePlan(
	tableName string,
	tx *transaction.Transaction,
	mdm *metadata.Manager) (*TablePlan, error) {
	layout, err := mdm.Layout(tableName, tx)
	if err != nil {
		return nil, fmt.Errorf("mdm.Layout error: %w", err)
	}
	statInfo, err := mdm.StatInfo(tableName, layout, tx)
	if err != nil {
		return nil, fmt.Errorf("mdm.StatInfo error: %w", err)
	}
	return &TablePlan{
		tableName: tableName,
		tx:        tx,
		layout:    layout,
		statInfo:  statInfo,
	}, nil
}

func (TablePlan) Result() {}

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

func (t TablePlan) String() string {
	return fmt.Sprintf("Table{%s}", t.tableName)

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

func (ProductPlan) Result() {}

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

func (p ProductPlan) String() string {
	return fmt.Sprintf("Product(%v, %v)", p.p1, p.p2)
}

type SelectPlan struct {
	p    Plan
	pred query.Predicate
}

var _ Plan = (*SelectPlan)(nil)

func NewSelectPlan(p Plan, pred query.Predicate) *SelectPlan {
	return &SelectPlan{p: p, pred: pred}
}

func (SelectPlan) Result() {}

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

func (s SelectPlan) String() string {
	return fmt.Sprintf("Select{%s}(%v)", s.pred, s.p)
}

type ProjectPlan struct {
	p    Plan
	sche schema.Schema
}

var _ Plan = (*ProjectPlan)(nil)

func NewProjectPlan(p Plan, fieldNames []schema.FieldName) *ProjectPlan {
	s := schema.NewSchema()
	for _, name := range fieldNames {
		s.Add(name, *p.Schema())
	}
	return &ProjectPlan{p: p, sche: s}
}

func (ProjectPlan) Result() {}

func (p ProjectPlan) Open() (query.Scan, error) {
	s, err := p.p.Open()
	if err != nil {
		return nil, fmt.Errorf("p.Open error: %w", err)
	}
	return query.NewProjectScan(s, p.sche.FieldNames()...), nil
}

func (p ProjectPlan) BlockAccessed() int {
	return p.p.BlockAccessed()
}

func (p ProjectPlan) RecordsOutput() int {
	return p.p.RecordsOutput()
}

func (p ProjectPlan) DistinctValues(fieldName schema.FieldName) int {
	return p.p.DistinctValues(fieldName)
}

func (p ProjectPlan) Schema() *schema.Schema {
	return &p.sche
}

func (p ProjectPlan) String() string {
	fieldNames := make([]string, len(p.sche.FieldNames()))
	for i, name := range p.sche.FieldNames() {
		fieldNames[i] = string(name)
	}
	return fmt.Sprintf("Project{%s}(%v)", strings.Join(fieldNames, ","), p.p)
}
