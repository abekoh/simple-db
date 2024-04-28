package schema

import "github.com/abekoh/simple-db/internal/query"

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
	s.AddField(name, Field{typ: Integer32, length: 0})
}

func (s *Schema) AddStrField(name FieldName, length int32) {
	s.AddField(name, Field{typ: Varchar, length: length})
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

func (f FieldName) Evaluate(scan query.Scan) (query.Constant, error) {
	return scan.Val(f)
}
