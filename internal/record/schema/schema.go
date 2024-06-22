package schema

import (
	"fmt"
)

type RID struct {
	blockNum int32
	slot     int32
}

func NewRID(blockNum, slot int32) RID {
	return RID{blockNum: blockNum, slot: slot}
}

func (r RID) BlockNum() int32 {
	return r.blockNum
}

func (r RID) Slot() int32 {
	return r.slot
}

func (r RID) String() string {
	return fmt.Sprintf("RID{blockNum=%d, slot=%d}", r.blockNum, r.slot)
}

type FieldType int32

const (
	Integer32 FieldType = iota
	Varchar
)

type Flag int32

const (
	Empty Flag = iota
	Used
)

type Field struct {
	typ    FieldType
	length int32
}

func NewField(typ FieldType, length int32) Field {
	return Field{typ: typ, length: length}
}

func NewInt32Field() Field {
	return Field{typ: Integer32, length: 0}
}

func NewVarcharField(length int32) Field {
	return Field{typ: Varchar, length: length}
}

type Schema struct {
	fields    []FieldName
	fieldsMap map[FieldName]Field
}

func NewSchema() Schema {
	return Schema{
		fields:    make([]FieldName, 0),
		fieldsMap: make(map[FieldName]Field),
	}
}

func (s *Schema) AddField(name FieldName, f Field) {
	s.fields = append(s.fields, name)
	s.fieldsMap[name] = f
}

func (s *Schema) AddInt32Field(name FieldName) {
	s.AddField(name, NewInt32Field())
}

func (s *Schema) AddStrField(name FieldName, length int32) {
	s.AddField(name, NewVarcharField(length))
}

func (s *Schema) Add(name FieldName, schema Schema) {
	typ := schema.Typ(name)
	length := schema.Length(name)
	s.AddField(name, Field{typ: typ, length: length})
}

func (s *Schema) AddAll(schema Schema) {
	for _, field := range schema.fields {
		f, ok := schema.fieldsMap[field]
		if !ok {
			panic("field not found")
		}
		s.AddField(field, f)
	}
}

func (s *Schema) FieldNames() []FieldName {
	names := make([]FieldName, 0, len(s.fieldsMap))
	for name := range s.fieldsMap {
		names = append(names, name)
	}
	return names
}

func (s *Schema) HasField(name FieldName) bool {
	_, ok := s.fieldsMap[name]
	return ok
}

func (s *Schema) Typ(name FieldName) FieldType {
	return s.fieldsMap[name].typ
}

func (s *Schema) Length(name FieldName) int32 {
	return s.fieldsMap[name].length
}

type FieldName string

func (f FieldName) Evaluate(v Valuable) (Constant, error) {
	return v.Val(f)
}

type Constant interface {
	fmt.Stringer
	Val() any
}

type ConstantInt32 int32

func (v ConstantInt32) String() string {
	return fmt.Sprintf("%d", v)
}

func (v ConstantInt32) Val() any {
	return int32(v)
}

func (v ConstantInt32) Evaluate(Valuable) (Constant, error) {
	return v, nil
}

type ConstantStr string

func (v ConstantStr) String() string {
	return string(v)
}

func (v ConstantStr) Val() any {
	return string(v)
}

func (v ConstantStr) Evaluate(Valuable) (Constant, error) {
	return v, nil
}

type Valuable interface {
	Val(fieldName FieldName) (Constant, error)
}
