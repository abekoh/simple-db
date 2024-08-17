package plan

import (
	"fmt"
	"maps"
	"slices"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
)

type GroupByScan struct {
	scan             query.Scan
	groupFields      []schema.FieldName
	groupValues      map[schema.FieldName]schema.Constant
	aggregationFuncs []query.AggregationFunc
	moreGroups       bool
}

var _ query.Scan = (*GroupByScan)(nil)

func NewGroupByScan(scan query.Scan, fields []schema.FieldName, aggregationFuncs []query.AggregationFunc) (*GroupByScan, error) {
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
	return nil, ErrFieldNotFound
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
		return 0, schema.ErrTypeAssertionFailed
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
		return "", schema.ErrTypeAssertionFailed
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
	p                Plan
	groupFields      []schema.FieldName
	aggregationFuncs []query.AggregationFunc
	sche             schema.Schema
}

var _ Plan = (*GroupByPlan)(nil)

func NewGroupByPlan(tx *transaction.Transaction, p Plan, groupFields []schema.FieldName, aggregationFuncs []query.AggregationFunc) *GroupByPlan {
	s := schema.NewSchema()
	for _, fn := range groupFields {
		s.Add(fn, *p.Schema())
	}
	for _, f := range aggregationFuncs {
		s.AddInt32Field(f.AliasName())
	}
	order := make(query.Order, len(groupFields))
	for i, f := range groupFields {
		order[i] = query.OrderElement{
			Field:     f,
			OrderType: query.Asc,
		}
	}
	return &GroupByPlan{
		p:                NewSortPlan(tx, p, order),
		groupFields:      groupFields,
		aggregationFuncs: aggregationFuncs,
		sche:             s,
	}
}

func (g GroupByPlan) Result() {}

func (g GroupByPlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return g.p.Placeholders(findSchema)
}

func (g GroupByPlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	newP, err := g.p.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("g.p.SwapParams error: %w", err)
	}
	return &BoundPlan{
		Plan: newP.(Plan),
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

func (g GroupByPlan) Info() Info {
	conditions := make(map[string][]string)
	for _, f := range g.groupFields {
		conditions["groupFields"] = append(conditions["groupFields"], string(f))
	}
	for _, f := range g.aggregationFuncs {
		conditions["aggregationFuncs"] = append(conditions["aggregationFuncs"], f.String())
	}
	return Info{
		NodeType:      "GroupBy",
		Conditions:    conditions,
		BlockAccessed: g.BlockAccessed(),
		RecordsOutput: g.RecordsOutput(),
		Children:      []Info{g.p.Info()},
	}
}
