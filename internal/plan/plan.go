package plan

import (
	"encoding/json"
	"fmt"

	"github.com/abekoh/simple-db/internal/index"
	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Config struct {
	QueryPlannerInitializer  QueryPlannerInitializer
	UpdatePlannerInitializer UpdatePlannerInitializer
}

type Result interface {
	Result()
}

type CommandResult struct {
	Type  CommandType
	Count int
}

func (CommandResult) Result() {}

type CommandType int

const (
	Insert CommandType = iota
	Delete
	Update
	CreateTable
	CreateView
	CreateIndex
	Begin
	Commit
	Rollback
)

type Info struct {
	NodeType      string              `json:"nodeType"`
	Conditions    map[string][]string `json:"conditions,omitempty"`
	BlockAccessed int                 `json:"blockAccessed"`
	RecordsOutput int                 `json:"recordsOutput"`
	Children      []Info              `json:"children,omitempty"`
}

func (i Info) JSON() json.RawMessage {
	b, _ := json.Marshal(i)
	return b
}

type Plan interface {
	Result
	statement.Prepared
	Open() (query.Scan, error)
	BlockAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName schema.FieldName) int
	Schema() *schema.Schema
	Info() Info
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

func (t TablePlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return nil
}

func (t TablePlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	return BoundPlan{Plan: t}, nil
}

func (t TablePlan) Info() Info {
	return Info{
		NodeType: "Table",
		Conditions: map[string][]string{
			"table": {t.tableName},
		},
		BlockAccessed: t.BlockAccessed(),
		RecordsOutput: t.RecordsOutput(),
	}
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

func (p ProductPlan) Info() Info {
	return Info{
		NodeType:      "Product",
		BlockAccessed: p.BlockAccessed(),
		RecordsOutput: p.RecordsOutput(),
		Children: []Info{
			p.p1.Info(),
			p.p2.Info(),
		},
	}
}

func (p ProductPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	placeholders := p.p1.Placeholders(findSchema)
	for k, v := range p.p2.Placeholders(findSchema) {
		placeholders[k] = v
	}
	return placeholders
}

func (p ProductPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	b1, err := p.p1.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p1.SwapParams error: %w", err)
	}
	b2, err := p.p2.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p2.SwapParams error: %w", err)
	}
	return &BoundPlan{
		Plan: &ProductPlan{
			p1:   b1.(Plan),
			p2:   b2.(Plan),
			sche: p.sche,
		},
	}, nil
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
	if _, ok := s.pred.EquatesWithConstant(fieldName); ok {
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

func (s SelectPlan) Info() Info {
	return Info{
		NodeType: "Select",
		Conditions: map[string][]string{
			"predicate": {s.pred.String()},
		},
		BlockAccessed: s.BlockAccessed(),
		RecordsOutput: s.RecordsOutput(),
		Children: []Info{
			s.p.Info(),
		},
	}
}

func (s SelectPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	placeholders := make(map[int]schema.FieldType)
	for _, t := range s.pred {
		lhs, rhs := t.Expressions()
		if p, ok := lhs.(schema.Placeholder); ok {
			if fn, ok := rhs.(schema.FieldName); ok {
				placeholders[int(p)] = s.Schema().Typ(fn)
			}
		}
		if p, ok := rhs.(schema.Placeholder); ok {
			if fn, ok := lhs.(schema.FieldName); ok {
				placeholders[int(p)] = s.Schema().Typ(fn)
			}
		}
	}
	return placeholders
}

func (s SelectPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	bp, err := s.p.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p.SwapParams error: %w", err)
	}
	pred, err := s.pred.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("pred.SwapParams error: %w", err)
	}
	return &BoundPlan{
		Plan: &SelectPlan{
			p:    bp.(Plan),
			pred: pred,
		},
	}, nil
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

func (p ProjectPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return p.p.Placeholders(findSchema)
}

func (p ProjectPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	b, err := p.p.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p.SwapParams error: %w", err)
	}
	return &BoundPlan{Plan: &ProjectPlan{p: b.(Plan), sche: p.sche}}, nil
}

func (p ProjectPlan) Info() Info {
	conditions := make(map[string][]string)
	for _, name := range p.sche.FieldNames() {
		conditions["fields"] = append(conditions["fields"], string(name))
	}
	return Info{
		NodeType:      "Project",
		Conditions:    conditions,
		BlockAccessed: p.BlockAccessed(),
		RecordsOutput: p.RecordsOutput(),
		Children: []Info{
			p.p.Info(),
		},
	}
}

type BoundPlan struct {
	Plan
}

var _ Plan = (*BoundPlan)(nil)

func (BoundPlan) Bound() {}

type IndexSelectPlan struct {
	p         Plan
	indexInfo *metadata.IndexInfo
	val       schema.Constant
}

func NewIndexSelectPlan(p Plan, indexInfo *metadata.IndexInfo, val schema.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{p: p, indexInfo: indexInfo, val: val}
}

var _ Plan = (*IndexSelectPlan)(nil)

func (i IndexSelectPlan) Result() {}

func (i IndexSelectPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return i.p.Placeholders(findSchema)
}

func (i IndexSelectPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	b, err := i.p.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p.SwapParams error: %w", err)
	}
	placeholderVal, ok := i.val.(schema.Placeholder)
	if !ok {
		return nil, fmt.Errorf("failed to cast to schema.ConstantInt32 from %T", i.val)
	}
	newVal, ok := params[int(placeholderVal)]
	if !ok {
		newVal = i.val
	}
	return BoundPlan{
		Plan: NewIndexSelectPlan(b.(Plan), i.indexInfo, newVal),
	}, nil
}

func (i IndexSelectPlan) Open() (query.Scan, error) {
	s, err := i.p.Open()
	if err != nil {
		return nil, fmt.Errorf("p.Open error: %w", err)
	}
	ts, ok := s.(*record.TableScan)
	if !ok {
		return nil, fmt.Errorf("failed to cast to *record.TableScan from %T", s)
	}
	idx, err := i.indexInfo.Open()
	if err != nil {
		return nil, fmt.Errorf("indexInfo.Open error: %w", err)
	}
	is, err := index.NewSelectScan(ts, idx, i.val)
	if err != nil {
		return nil, fmt.Errorf("index.NewSelectScan error: %w", err)
	}
	return is, nil
}

func (i IndexSelectPlan) BlockAccessed() int {
	return i.indexInfo.BlockAccessed()
}

func (i IndexSelectPlan) RecordsOutput() int {
	return i.indexInfo.RecordsOutput()
}

func (i IndexSelectPlan) DistinctValues(fieldName schema.FieldName) int {
	return i.indexInfo.DistinctValues(fieldName)
}

func (i IndexSelectPlan) Schema() *schema.Schema {
	return i.p.Schema()
}

func (i IndexSelectPlan) Info() Info {
	return Info{
		NodeType: "IndexSelect",
		Conditions: map[string][]string{
			"index": {i.indexInfo.IndexName()},
			"value": {i.val.String()},
		},
		BlockAccessed: i.BlockAccessed(),
		RecordsOutput: i.RecordsOutput(),
		Children: []Info{
			i.p.Info(),
		},
	}
}

type IndexJoinPlan struct {
	p1, p2    Plan
	indexInfo *metadata.IndexInfo
	joinField schema.FieldName
	sche      schema.Schema
}

func NewIndexJoinPlan(p1, p2 Plan, indexInfo *metadata.IndexInfo, joinField schema.FieldName) *IndexJoinPlan {
	s := schema.NewSchema()
	s.AddAll(*p1.Schema())
	s.AddAll(*p2.Schema())
	return &IndexJoinPlan{p1: p1, p2: p2, indexInfo: indexInfo, joinField: joinField, sche: s}
}

var _ Plan = (*IndexJoinPlan)(nil)

func (i IndexJoinPlan) Result() {}

func (i IndexJoinPlan) Info() Info {
	return Info{
		NodeType: "IndexJoin",
		Conditions: map[string][]string{
			"index": {i.indexInfo.IndexName()},
			"field": {string(i.joinField)},
		},
		BlockAccessed: i.BlockAccessed(),
		RecordsOutput: i.RecordsOutput(),
		Children: []Info{
			i.p1.Info(),
			i.p2.Info(),
		},
	}
}

func (i IndexJoinPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	placeholders := i.p1.Placeholders(findSchema)
	for k, v := range i.p2.Placeholders(findSchema) {
		placeholders[k] = v
	}
	return placeholders
}

func (i IndexJoinPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	b1, err := i.p1.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p1.SwapParams error: %w", err)
	}
	b2, err := i.p2.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("p2.SwapParams error: %w", err)
	}
	return &BoundPlan{
		Plan: &IndexJoinPlan{
			p1:        b1.(Plan),
			p2:        b2.(Plan),
			indexInfo: i.indexInfo,
			joinField: i.joinField,
			sche:      i.sche,
		},
	}, nil
}

func (i IndexJoinPlan) Open() (query.Scan, error) {
	s1, err := i.p1.Open()
	if err != nil {
		return nil, fmt.Errorf("p1.Open error: %w", err)
	}
	s2, err := i.p2.Open()
	if err != nil {
		return nil, fmt.Errorf("p1.Open error: %w", err)
	}
	ts2, ok := s2.(*record.TableScan)
	if !ok {
		return nil, fmt.Errorf("failed to cast to *record.TableScan from %T", s2)
	}
	idx, err := i.indexInfo.Open()
	if err != nil {
		return nil, fmt.Errorf("indexInfo.Open error: %w", err)
	}
	js, err := index.NewJoinScan(s1, ts2, idx, i.joinField)
	if err != nil {
		return nil, fmt.Errorf("index.NewJoinScan error: %w", err)
	}
	return js, nil
}

func (i IndexJoinPlan) BlockAccessed() int {
	return i.p1.BlockAccessed() + (i.p1.RecordsOutput() * i.indexInfo.BlockAccessed()) + i.RecordsOutput()
}

func (i IndexJoinPlan) RecordsOutput() int {
	return i.p1.RecordsOutput() * i.indexInfo.RecordsOutput()
}

func (i IndexJoinPlan) DistinctValues(fieldName schema.FieldName) int {
	if i.p1.Schema().HasField(fieldName) {
		return i.p1.DistinctValues(fieldName)
	} else {
		return i.p2.DistinctValues(fieldName)
	}
}

func (i IndexJoinPlan) Schema() *schema.Schema {
	return &i.sche
}
