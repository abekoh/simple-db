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

func NewLayoutSchema(schema Schema) *Layout {
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
	return &Layout{schema: schema, offsets: offsets, slotSize: pos}
}

func NewLayout(schema Schema, offsets map[string]int32, slotSize int32) *Layout {
	return &Layout{schema: schema, offsets: offsets, slotSize: slotSize}
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
	layout  *Layout
}

func NewRecordPage(tx *transaction.Transaction, blockID file.BlockID, layout *Layout) (*RecordPage, error) {
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

func (rp *RecordPage) NextAfter(slot int32) (int32, bool, error) {
	return rp.searchAfter(slot, Used)
}

func (rp *RecordPage) InsertAfter(slot int32) (int32, bool, error) {
	newSlot, ok, err := rp.searchAfter(slot, Empty)
	if err != nil {
		return -1, false, fmt.Errorf("could not search after: %w", err)
	}
	if ok {
		if err := rp.setFlag(newSlot, Used); err != nil {
			return -1, ok, fmt.Errorf("could not set flag: %w", err)
		}
	}
	return newSlot, ok, nil
}

func (rp *RecordPage) offset(slot int32) int32 {
	return slot * rp.layout.SlotSize()
}

func (rp *RecordPage) isValidSlot(slot int32) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) searchAfter(slot int32, flag Flag) (int32, bool, error) {
	slot++
	for rp.isValidSlot(slot) {
		val, err := rp.tx.Int32(rp.blockID, rp.offset(slot))
		if err != nil {
			return -1, false, fmt.Errorf("could not read int32: %w", err)
		}
		if Flag(val) == flag {
			return slot, true, nil
		}
		slot++
	}
	return -1, false, nil
}

func (rp *RecordPage) setFlag(slot int32, flag Flag) error {
	if err := rp.tx.SetInt32(rp.blockID, rp.offset(slot), int32(flag), true); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

type TableScan struct {
	tx          *transaction.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentSlot int32
}

func NewTableScan(tx *transaction.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	ts := &TableScan{tx: tx, layout: layout, filename: fmt.Sprintf("%s.tbl", tableName)}
	l, err := tx.Size(ts.filename)
	if err != nil {
		return nil, fmt.Errorf("could not get size: %w", err)
	}
	if l == 0 {
		if err := ts.moveToNewBlock(); err != nil {
			return nil, fmt.Errorf("could not move to new block: %w", err)
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, fmt.Errorf("could not move to block: %w", err)
		}
	}
	return ts, nil
}

func (ts *TableScan) moveToNewBlock() error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("could not close: %w", err)
	}
	blockID, err := ts.tx.Append(ts.filename)
	if err != nil {
		return fmt.Errorf("could not append: %w", err)
	}
	rp, err := NewRecordPage(ts.tx, blockID, ts.layout)
	if err != nil {
		return fmt.Errorf("could not create new record page: %w", err)
	}
	ts.rp = rp
	if err := ts.rp.Format(); err != nil {
		return fmt.Errorf("could not format: %w", err)
	}
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) moveToBlock(blockNum int32) error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("could not close: %w", err)
	}
	blockID := file.NewBlockID(ts.filename, blockNum)
	rp, err := NewRecordPage(ts.tx, blockID, ts.layout)
	if err != nil {
		return fmt.Errorf("could not create new record page: %w", err)
	}
	ts.rp = rp
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	s, err := ts.tx.Size(ts.filename)
	if err != nil {
		return false, fmt.Errorf("could not get size: %w", err)
	}
	return ts.rp.blockID.Num() == s-1, nil
}

func (ts *TableScan) Close() error {
	if ts.rp != nil {
		if err := ts.tx.Unpin(ts.rp.blockID); err != nil {
			return fmt.Errorf("failed to unpin: %w", err)
		}
	}
	return nil
}

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() (bool, error) {
	cs, ok, err := ts.rp.NextAfter(ts.currentSlot)
	if err != nil {
		return false, fmt.Errorf("could not next after: %w", err)
	}
	for !ok {
		lastBlock, err := ts.atLastBlock()
		if err != nil {
			return false, fmt.Errorf("could not at last block: %w", err)
		}
		if lastBlock {
			return false, nil
		}
		if err := ts.moveToBlock(ts.rp.blockID.Num() + 1); err != nil {
			return false, fmt.Errorf("could not move to block: %w", err)
		}
		cs, ok, err = ts.rp.NextAfter(cs)
		if err != nil {
			return false, fmt.Errorf("could not next after: %w", err)
		}
	}
	ts.currentSlot = cs
	return true, nil
}

func (ts *TableScan) Int32(fieldName string) (int32, error) {
	r, err := ts.rp.Int32(ts.currentSlot, fieldName)
	if err != nil {
		return 0, fmt.Errorf("could not read int32: %w", err)
	}
	return r, nil
}

func (ts *TableScan) Str(fieldName string) (string, error) {
	r, err := ts.rp.Str(ts.currentSlot, fieldName)
	if err != nil {
		return "", fmt.Errorf("could not read string: %w", err)
	}
	return r, nil
}

func (ts *TableScan) Val(fieldName string) (any, error) {
	switch ts.layout.Schema().Typ(fieldName) {
	case Integer32:
		return ts.Int32(fieldName)
	case Varchar:
		return ts.Str(fieldName)
	}
	return nil, fmt.Errorf("unknown type")
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) SetInt32(fieldName string, val int32) error {
	if err := ts.rp.SetInt32(ts.currentSlot, fieldName, val); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

func (ts *TableScan) SetStr(fieldName, val string) error {
	if err := ts.rp.SetStr(ts.currentSlot, fieldName, val); err != nil {
		return fmt.Errorf("could not set string: %w", err)
	}
	return nil
}

func (ts *TableScan) SetVal(fieldName string, val any) error {
	switch v := val.(type) {
	case int32:
		return ts.SetInt32(fieldName, v)
	case string:
		return ts.SetStr(fieldName, v)
	}
	return fmt.Errorf("unknown type")
}

func (ts *TableScan) Insert() error {
	cs, ok, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return fmt.Errorf("could not insert after: %w", err)
	}
	if !ok {
		lastBlock, err := ts.atLastBlock()
		if err != nil {
			return fmt.Errorf("could not at last block: %w", err)
		}
		if lastBlock {
			if err := ts.moveToNewBlock(); err != nil {
				return fmt.Errorf("could not move to new block: %w", err)
			}
		} else {
			if err := ts.moveToBlock(ts.rp.blockID.Num() + 1); err != nil {
				return fmt.Errorf("could not move to block: %w", err)
			}
		}
		cs, ok, err = ts.rp.InsertAfter(ts.currentSlot)
		if err != nil {
			return fmt.Errorf("could not insert after: %w", err)
		}
	}
	ts.currentSlot = cs
	return nil
}

func (ts *TableScan) Delete() error {
	if err := ts.rp.Delete(ts.currentSlot); err != nil {
		return fmt.Errorf("could not delete: %w", err)
	}
	return nil
}
