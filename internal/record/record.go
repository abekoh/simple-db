package record

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/transaction"
)

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

func NewSchema() Schema {
	return Schema{fields: make(map[string]Field)}
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

type Layout struct {
	schema   Schema
	offsets  map[string]int
	slotSize int
}

func NewLayoutSchema(schema Schema) Layout {
	offsets := make(map[string]int)
	pos := 4
	for _, name := range schema.FieldNames() {
		offsets[name] = pos
		switch schema.Typ(name) {
		case Integer32:
			pos += 4
		case Varchar:
			pos += int(file.PageStrMaxLength(schema.Length(name)))
		}
	}
	return Layout{schema: schema, offsets: offsets, slotSize: pos}
}

func NewLayout(schema Schema, offsets map[string]int, slotSize int) Layout {
	return Layout{schema: schema, offsets: offsets, slotSize: slotSize}
}

func (l Layout) Schema() Schema {
	return l.schema
}

func (l Layout) Offset(name string) int {
	return l.offsets[name]
}

func (l Layout) SlotSize() int {
	return l.slotSize
}

type RecordPage struct {
	tx      *transaction.Transaction
	blockID file.BlockID
	layout  Layout
}

func NewRecordPage(tx *transaction.Transaction, blockID file.BlockID, layout Layout) (*RecordPage, error) {
	if _, err := tx.Pin(blockID); err != nil {
		return nil, fmt.Errorf("could not pin block: %w", err)
	}
	return &RecordPage{tx: tx, blockID: blockID, layout: layout}, nil
}

func (rp *RecordPage) Int32(slot int, fieldName string) (int32, error) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	val, err := rp.tx.Int32(rp.blockID, int32(fieldPos))
	if err != nil {
		return 0, fmt.Errorf("could not read int32: %w", err)
	}
	return val, nil
}

func (rp *RecordPage) Str(slot int, fieldName string) (string, error) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	val, err := rp.tx.Str(rp.blockID, int32(fieldPos))
	if err != nil {
		return "", fmt.Errorf("could not read string: %w", err)
	}
	return val, nil
}

func (rp *RecordPage) SetInt32(slot int, fieldName string, n int32) error {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	if err := rp.tx.SetInt32(rp.blockID, int32(fieldPos), n, false); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

func (rp *RecordPage) SetStr(slot int, fieldName, s string) error {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldName)
	if err := rp.tx.SetStr(rp.blockID, int32(fieldPos), s, false); err != nil {
		return fmt.Errorf("could not set string: %w", err)
	}
	return nil
}

func (rp *RecordPage) Format() {
	slot := 0
	for rp.isValidSlot(slot) {
	}
}

func (rp *RecordPage) offset(slot int) int {
	return slot * rp.layout.SlotSize()
}

func (rp *RecordPage) isValidSlot(slot int) bool {
	return int32(rp.offset(slot+1)) <= rp.tx.BlockSize()
}
