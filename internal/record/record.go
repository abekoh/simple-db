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

type Flag int32

const (
	Empty Flag = iota
	Used
)

type Field struct {
	typ    FieldType
	length int32
}

type Schema struct {
	fields    []string
	fieldsMap map[string]Field
}

func NewSchema() Schema {
	return Schema{
		fields:    make([]string, 0),
		fieldsMap: make(map[string]Field),
	}
}

func (s *Schema) addField(name string, f Field) {
	s.fields = append(s.fields, name)
	s.fieldsMap[name] = f
}

func (s *Schema) AddInt32Field(name string) {
	s.addField(name, Field{typ: Integer32, length: 0})
}

func (s *Schema) AddStrField(name string, length int32) {
	s.addField(name, Field{typ: Varchar, length: length})
}

func (s *Schema) Add(name string, schema Schema) {
	typ := schema.Typ(name)
	length := schema.Length(name)
	s.addField(name, Field{typ: typ, length: length})
}

func (s *Schema) AddAll(schema Schema) {
	for _, field := range schema.fields {
		f, ok := schema.fieldsMap[field]
		if !ok {
			panic("field not found")
		}
		s.addField(field, f)
	}
}

func (s *Schema) FieldNames() []string {
	names := make([]string, 0, len(s.fieldsMap))
	for name := range s.fieldsMap {
		names = append(names, name)
	}
	return names
}

func (s *Schema) HasField(name string) bool {
	_, ok := s.fieldsMap[name]
	return ok
}

func (s *Schema) Typ(name string) FieldType {
	return s.fieldsMap[name].typ
}

func (s *Schema) Length(name string) int32 {
	return s.fieldsMap[name].length
}

type Layout struct {
	schema   Schema
	offsets  map[string]int32
	slotSize int32
}

func NewLayoutSchema(schema Schema) Layout {
	offsets := make(map[string]int32)
	var pos int32 = 4
	for _, name := range schema.FieldNames() {
		offsets[name] = pos
		switch schema.Typ(name) {
		case Integer32:
			pos += 4
		case Varchar:
			pos += file.PageStrMaxLength(schema.Length(name))
		}
	}
	return Layout{schema: schema, offsets: offsets, slotSize: pos}
}

func NewLayout(schema Schema, offsets map[string]int32, slotSize int32) Layout {
	return Layout{schema: schema, offsets: offsets, slotSize: slotSize}
}

func (l Layout) Schema() *Schema {
	return &l.schema
}

func (l Layout) Offset(name string) (int32, bool) {
	r, ok := l.offsets[name]
	return r, ok
}

func (l Layout) SlotSize() int32 {
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

func (rp *RecordPage) Int32(slot int32, fieldName string) (int32, error) {
	layoutOffset, ok := rp.layout.Offset(fieldName)
	if !ok {
		return 0, fmt.Errorf("field not found: %s", fieldName)

	}
	val, err := rp.tx.Int32(rp.blockID, rp.offset(slot)+layoutOffset)
	if err != nil {
		return 0, fmt.Errorf("could not read int32: %w", err)
	}
	return val, nil
}

func (rp *RecordPage) Str(slot int32, fieldName string) (string, error) {
	layoutOffset, ok := rp.layout.Offset(fieldName)
	if !ok {
		return "", fmt.Errorf("field not found: %s", fieldName)
	}
	val, err := rp.tx.Str(rp.blockID, rp.offset(slot)+layoutOffset)
	if err != nil {
		return "", fmt.Errorf("could not read string: %w", err)
	}
	return val, nil
}

func (rp *RecordPage) SetInt32(slot int32, fieldName string, val int32) error {
	layoutOffset, ok := rp.layout.Offset(fieldName)
	if !ok {
		return fmt.Errorf("field not found: %s", fieldName)
	}
	if err := rp.tx.SetInt32(rp.blockID, rp.offset(slot)+layoutOffset, val, true); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

func (rp *RecordPage) SetStr(slot int32, fieldName, val string) error {
	layoutOffset, ok := rp.layout.Offset(fieldName)
	if !ok {
		return fmt.Errorf("field not found: %s", fieldName)
	}
	// TODO: validate length
	if err := rp.tx.SetStr(rp.blockID, rp.offset(slot)+layoutOffset, val, true); err != nil {
		return fmt.Errorf("could not set string: %w", err)
	}
	return nil
}

func (rp *RecordPage) Delete(slot int32) error {
	if err := rp.setFlag(slot, Empty); err != nil {
		return fmt.Errorf("could not set flag: %w", err)
	}
	return nil
}

func (rp *RecordPage) Format() error {
	var slot int32
	for rp.isValidSlot(slot) {
		if err := rp.tx.SetInt32(rp.blockID, rp.offset(slot), int32(Empty), false); err != nil {
			return fmt.Errorf("could not format: %w", err)
		}
		schema := rp.layout.Schema()
		for _, name := range schema.FieldNames() {
			layoutOffset, ok := rp.layout.Offset(name)
			if !ok {
				return fmt.Errorf("field not found: %s", name)
			}
			fieldPos := rp.offset(slot) + layoutOffset
			switch schema.Typ(name) {
			case Integer32:
				if err := rp.tx.SetInt32(rp.blockID, fieldPos, 0, false); err != nil {
					return fmt.Errorf("could not format: %w", err)
				}
			case Varchar:
				if err := rp.tx.SetStr(rp.blockID, fieldPos, "", false); err != nil {
					return fmt.Errorf("could not format: %w", err)
				}
			}
		}
		slot++
	}
	return nil
}

func (rp *RecordPage) NextAfter(slot int32) (int32, error) {
	return rp.searchAfter(slot, Empty)
}

func (rp *RecordPage) InsertAfter(slot int32) (int32, error) {
	newSlot, err := rp.searchAfter(slot, Empty)
	if err != nil {
		return -1, fmt.Errorf("could not search after: %w", err)
	}
	if newSlot >= 0 {
		if err := rp.setFlag(newSlot, Used); err != nil {
			return -1, fmt.Errorf("could not set flag: %w", err)
		}
	}
	return newSlot, nil
}

func (rp *RecordPage) offset(slot int32) int32 {
	return slot * rp.layout.SlotSize()
}

func (rp *RecordPage) isValidSlot(slot int32) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) searchAfter(slot int32, flag Flag) (int32, error) {
	slot++
	for rp.isValidSlot(slot) {
		val, err := rp.tx.Int32(rp.blockID, rp.offset(slot))
		if err != nil {
			return -1, fmt.Errorf("could not read int32: %w", err)
		}
		if val == int32(flag) {
			return slot, nil
		}
		slot++
	}
	return -1, nil
}

func (rp *RecordPage) setFlag(slot int32, flag Flag) error {
	if err := rp.tx.SetInt32(rp.blockID, rp.offset(slot), int32(flag), true); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}
