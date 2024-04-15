package record

type FieldType int

const (
	Integer32 FieldType = iota
	Varchar
)

type Field struct {
	typ    FieldType
	length int
}

type Schema struct {
	fields map[string]Field
}

func (s *Schema) AddField(name string, f Field) {
	s.fields[name] = f
}

func (s *Schema) AddInt32Field(name string) {
	s.AddField(name, Field{typ: Integer32, length: 0})
}

func (s *Schema) AddStrField(name string, length int) {
	s.AddField(name, Field{typ: Varchar, length: length})
}

func (s *Schema) Add(name string, schema Schema) {
	typ := schema.Typ(name)
	length := schema.Length(name)
	s.AddField(name, Field{typ: typ, length: length})
}

func (s *Schema) AddAll(schema Schema) {
	for name, field := range schema.fields {
		s.AddField(name, field)
	}
}

func (s *Schema) FieldNames() []string {
	names := make([]string, 0, len(s.fields))
	for name := range s.fields {
		names = append(names, name)
	}
	return names
}

func (s *Schema) HasField(name string) bool {
	_, ok := s.fields[name]
	return ok
}

func (s *Schema) Typ(name string) FieldType {
	return s.fields[name].typ
}

func (s *Schema) Length(name string) int {
	return s.fields[name].length
}
