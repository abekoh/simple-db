package materialize

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
)

type AggregationFunc interface {
	First(s query.Scan) error
	Next(s query.Scan) error
	AliasName() schema.FieldName
	Val() schema.Constant
}

type CountFunc struct {
	aliasName schema.FieldName
	count     int
}

var _ AggregationFunc = (*CountFunc)(nil)

func NewCountFunc(aliasName schema.FieldName) *CountFunc {
	return &CountFunc{aliasName: aliasName}
}

func (c *CountFunc) First(s query.Scan) error {
	c.count = 1
	return nil
}

func (c *CountFunc) Next(s query.Scan) error {
	c.count++
	return nil
}

func (c *CountFunc) AliasName() schema.FieldName {
	return c.aliasName
}

func (c *CountFunc) Val() schema.Constant {
	return schema.ConstantInt32(int32(c.count))
}

type MaxFunc struct {
	fieldName, aliasName schema.FieldName
	maxVal               schema.Constant
}

var _ AggregationFunc = (*MaxFunc)(nil)

func NewMaxFunc(fieldName, aliasName schema.FieldName) *MaxFunc {
	return &MaxFunc{fieldName: fieldName, aliasName: aliasName}
}

func (m *MaxFunc) First(s query.Scan) error {
	val, err := s.Val(m.fieldName)
	if err != nil {
		return err
	}
	m.maxVal = val
	return nil
}

func (m *MaxFunc) Next(s query.Scan) error {
	val, err := s.Val(m.fieldName)
	if err != nil {
		return err
	}
	if val.Compare(m.maxVal) > 0 {
		m.maxVal = val
	}
	return nil
}

func (m *MaxFunc) AliasName() schema.FieldName {
	return m.aliasName
}

func (m *MaxFunc) Val() schema.Constant {
	return m.maxVal
}

type GroupByScan struct {
	scan             query.Scan
	groupFields      []schema.FieldName
	groupValues      map[schema.FieldName]schema.Constant
	aggregationFuncs []AggregationFunc
	moreGroups       bool
}

var _ query.Scan = (*GroupByScan)(nil)

func NewGroupByScan(scan query.Scan, fields []schema.FieldName, aggregationFuncs []AggregationFunc) (*GroupByScan, error) {
	gs := GroupByScan{scan: scan, groupFields: fields, aggregationFuncs: aggregationFuncs}
	if err := gs.BeforeFirst(); err != nil {
		return nil, err
	}
	return &gs, nil
}

func (g *GroupByScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	if slices.Contains(g.groupFields, fieldName) {
		return g.groupValues[fieldName], nil
	}
	for _, f := range g.aggregationFuncs {
		if f.AliasName() == fieldName {
			return f.Val(), nil
		}
	}
	return nil, errors.New("field not found")
}

func (g *GroupByScan) BeforeFirst() error {
	if err := g.scan.BeforeFirst(); err != nil {
		return fmt.Errorf("g.scan.BeforeFirst error: %w", err)
	}
	ok, err := g.scan.Next()
	if err != nil {
		return fmt.Errorf("g.scan.Next error: %w", err)
	}
	g.moreGroups = ok
	return nil
}

func (g *GroupByScan) Next() (bool, error) {
	if !g.moreGroups {
		return false, nil
	}
	for _, f := range g.aggregationFuncs {
		if err := f.First(g.scan); err != nil {
			return false, fmt.Errorf("f.First error: %w", err)
		}
	}
	g.groupValues = make(map[schema.FieldName]schema.Constant)
	for _, f := range g.groupFields {
		val, err := g.scan.Val(f)
		if err != nil {
			return false, fmt.Errorf("g.scan.Val error: %w", err)
		}
		g.groupValues[f] = val
	}
	for {
		ok, err := g.scan.Next()
		if err != nil {
			return false, fmt.Errorf("g.scan.Next error: %w", err)
		}
		g.moreGroups = ok
		if !g.moreGroups {
			return true, nil
		}
		gv := make(map[schema.FieldName]schema.Constant)
		for _, f := range g.groupFields {
			val, err := g.scan.Val(f)
			if err != nil {
				return false, fmt.Errorf("g.scan.Val error: %w", err)
			}
			gv[f] = val
		}
		if !maps.Equal(g.groupValues, gv) {
			return true, nil
		}
		for _, f := range g.aggregationFuncs {
			if err := f.Next(g.scan); err != nil {
				return false, fmt.Errorf("f.Next error: %w", err)
			}
		}
	}
}

func (g *GroupByScan) Int32(fieldName schema.FieldName) (int32, error) {
	v, err := g.Val(fieldName)
	if err != nil {
		return 0, err
	}
	intV, ok := v.(schema.ConstantInt32)
	if !ok {
		return 0, errors.New("type assertion failed")
	}
	return int32(intV), nil
}

func (g *GroupByScan) Str(fieldName schema.FieldName) (string, error) {
	v, err := g.Val(fieldName)
	if err != nil {
		return "", err
	}
	strV, ok := v.(schema.ConstantStr)
	if !ok {
		return "", errors.New("type assertion failed")
	}
	return string(strV), nil
}

func (g *GroupByScan) HasField(fieldName schema.FieldName) bool {
	if slices.Contains(g.groupFields, fieldName) {
		return true
	}
	for _, f := range g.aggregationFuncs {
		if f.AliasName() == fieldName {
			return true
		}
	}
	return false
}

func (g *GroupByScan) Close() error {
	if err := g.scan.Close(); err != nil {
		return fmt.Errorf("g.scan.Close error: %w", err)
	}
	return nil
}

type GroupByPlan struct {
	p                plan.Plan
	groupFields      []schema.FieldName
	aggregationFuncs []AggregationFunc
	sche             schema.Schema
}

var _ plan.Plan = (*GroupByPlan)(nil)

func NewGroupByPlan(tx *transaction.Transaction, p plan.Plan, groupFields []schema.FieldName, aggregationFuncs []AggregationFunc) *GroupByPlan {
	s := schema.NewSchema()
	for _, fn := range groupFields {
		s.Add(fn, *p.Schema())
	}
	for _, f := range aggregationFuncs {
		s.AddInt32Field(f.AliasName())
	}
	return &GroupByPlan{
		p:                NewSortPlan(tx, p, groupFields),
		groupFields:      groupFields,
		aggregationFuncs: aggregationFuncs,
		sche:             s,
	}
}

func (g GroupByPlan) Result() {}

func (g GroupByPlan) String() string {
	// TODO implement me
	panic("implement me")
}

func (g GroupByPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return g.p.Placeholders(findSchema)
}

func (g GroupByPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	newP, err := g.p.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("g.p.SwapParams error: %w", err)
	}
	bp, ok := newP.(*plan.BoundPlan)
	if !ok {
		return nil, errors.New("type assertion failed")
	}
	return &plan.BoundPlan{
		Plan: bp,
	}, nil
}

func (g GroupByPlan) Open() (query.Scan, error) {
	s, err := g.p.Open()
	if err != nil {
		return nil, fmt.Errorf("g.p.Open error: %w", err)
	}
	gs, err := NewGroupByScan(s, g.groupFields, g.aggregationFuncs)
	if err != nil {
		return nil, fmt.Errorf("NewGroupByScan error: %w", err)
	}
	return gs, nil
}

func (g GroupByPlan) BlockAccessed() int {
	return g.p.BlockAccessed()
}

func (g GroupByPlan) RecordsOutput() int {
	numGroups := 1
	for _, f := range g.groupFields {
		numGroups *= g.p.DistinctValues(f)
	}
	return numGroups
}

func (g GroupByPlan) DistinctValues(fieldName schema.FieldName) int {
	if slices.Contains(g.groupFields, fieldName) {
		return g.p.DistinctValues(fieldName)
	}
	return g.RecordsOutput()
}

func (g GroupByPlan) Schema() *schema.Schema {
	return &g.sche
}
