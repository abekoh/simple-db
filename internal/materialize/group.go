package materialize

import (
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
)

type AggregationFunc interface {
	First(s query.Scan) error
	Next(s query.Scan) error
	FieldName() schema.FieldName
	Val() schema.Constant
}

type CountFunc struct {
	fieldName schema.FieldName
	count     int
}

var _ AggregationFunc = (*CountFunc)(nil)

func NewCountFunc(fieldName schema.FieldName) *CountFunc {
	return &CountFunc{fieldName: fieldName}
}

func (c *CountFunc) First(s query.Scan) error {
	c.count = 1
	return nil
}

func (c *CountFunc) Next(s query.Scan) error {
	c.count++
	return nil
}

func (c *CountFunc) FieldName() schema.FieldName {
	return c.fieldName
}

func (c *CountFunc) Val() schema.Constant {
	return schema.ConstantInt32(int32(c.count))
}

type MaxFunc struct {
	fieldName schema.FieldName
	maxVal    schema.Constant
}

var _ AggregationFunc = (*MaxFunc)(nil)

func NewMaxFunc(fieldName schema.FieldName) *MaxFunc {
	return &MaxFunc{fieldName: fieldName}
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

func (m *MaxFunc) FieldName() schema.FieldName {
	return m.fieldName
}

func (m *MaxFunc) Val() schema.Constant {
	return m.maxVal
}
