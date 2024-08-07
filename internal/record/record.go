package record

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Layout struct {
	sche     schema.Schema
	offsets  map[schema.FieldName]int32
	slotSize int32
}

func NewLayoutSchema(sche schema.Schema) *Layout {
	offsets := make(map[schema.FieldName]int32)
	var pos int32 = 4
	for _, name := range sche.FieldNames() {
		offsets[name] = pos
		switch sche.Typ(name) {
		case schema.Integer32:
			pos += 4
		case schema.Varchar:
			pos += file.PageStrMaxLength(sche.Length(name))
		}
	}
	return &Layout{sche: sche, offsets: offsets, slotSize: pos}
}

func NewLayout(schema schema.Schema, offsets map[schema.FieldName]int32, slotSize int32) *Layout {
	return &Layout{sche: schema, offsets: offsets, slotSize: slotSize}
}

func (l Layout) Schema() *schema.Schema {
	return &l.sche
}

func (l Layout) Offset(name schema.FieldName) (int32, bool) {
	r, ok := l.offsets[name]
	return r, ok
}

func (l Layout) SlotSize() int32 {
	return l.slotSize
}

type Page struct {
	tx      *transaction.Transaction
	blockID file.BlockID
	layout  *Layout
}

func NewPage(tx *transaction.Transaction, blockID file.BlockID, layout *Layout) (*Page, error) {
	if _, err := tx.Pin(blockID); err != nil {
		return nil, fmt.Errorf("could not pin block: %w", err)
	}
	return &Page{tx: tx, blockID: blockID, layout: layout}, nil
}

func (p *Page) Int32(slot int32, fieldName schema.FieldName) (int32, error) {
	layoutOffset, ok := p.layout.Offset(fieldName)
	if !ok {
		return 0, fmt.Errorf("field not found: %s", fieldName)

	}
	val, err := p.tx.Int32(p.blockID, p.offset(slot)+layoutOffset)
	if err != nil {
		return 0, fmt.Errorf("could not read int32: %w", err)
	}
	return val, nil
}

func (p *Page) Str(slot int32, fieldName schema.FieldName) (string, error) {
	layoutOffset, ok := p.layout.Offset(fieldName)
	if !ok {
		return "", fmt.Errorf("field not found: %s", fieldName)
	}
	val, err := p.tx.Str(p.blockID, p.offset(slot)+layoutOffset)
	if err != nil {
		return "", fmt.Errorf("could not read string: %w", err)
	}
	return val, nil
}

func (p *Page) SetInt32(slot int32, fieldName schema.FieldName, val int32) error {
	layoutOffset, ok := p.layout.Offset(fieldName)
	if !ok {
		return fmt.Errorf("field not found: %s", fieldName)
	}
	if err := p.tx.SetInt32(p.blockID, p.offset(slot)+layoutOffset, val, true); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

func (p *Page) SetStr(slot int32, fieldName schema.FieldName, val string) error {
	layoutOffset, ok := p.layout.Offset(fieldName)
	if !ok {
		return fmt.Errorf("field not found: %s", fieldName)
	}
	// TODO: validate length
	if err := p.tx.SetStr(p.blockID, p.offset(slot)+layoutOffset, val, true); err != nil {
		return fmt.Errorf("could not set string: %w", err)
	}
	return nil
}

func (p *Page) Delete(slot int32) error {
	if err := p.setFlag(slot, schema.Empty); err != nil {
		return fmt.Errorf("could not set flag: %w", err)
	}
	return nil
}

func (p *Page) Format() error {
	var slot int32
	for p.isValidSlot(slot) {
		if err := p.tx.SetInt32(p.blockID, p.offset(slot), int32(schema.Empty), false); err != nil {
			return fmt.Errorf("could not format: %w", err)
		}
		sche := p.layout.Schema()
		for _, name := range sche.FieldNames() {
			layoutOffset, ok := p.layout.Offset(name)
			if !ok {
				return fmt.Errorf("field not found: %s", name)
			}
			fieldPos := p.offset(slot) + layoutOffset
			switch sche.Typ(name) {
			case schema.Integer32:
				if err := p.tx.SetInt32(p.blockID, fieldPos, 0, false); err != nil {
					return fmt.Errorf("could not format: %w", err)
				}
			case schema.Varchar:
				if err := p.tx.SetStr(p.blockID, fieldPos, "", false); err != nil {
					return fmt.Errorf("could not format: %w", err)
				}
			}
		}
		slot++
	}
	return nil
}

func (p *Page) NextAfter(slot int32) (int32, bool, error) {
	return p.searchAfter(slot, schema.Used)
}

func (p *Page) InsertAfter(slot int32) (int32, bool, error) {
	newSlot, ok, err := p.searchAfter(slot, schema.Empty)
	if err != nil {
		return -1, false, fmt.Errorf("could not search after: %w", err)
	}
	if ok {
		if err := p.setFlag(newSlot, schema.Used); err != nil {
			return -1, ok, fmt.Errorf("could not set flag: %w", err)
		}
	}
	return newSlot, ok, nil
}

func (p *Page) BlockID() file.BlockID {
	return p.blockID
}

func (p *Page) offset(slot int32) int32 {
	return slot * p.layout.SlotSize()
}

func (p *Page) isValidSlot(slot int32) bool {
	return p.offset(slot+1) <= p.tx.BlockSize()
}

func (p *Page) searchAfter(slot int32, flag schema.Flag) (int32, bool, error) {
	slot++
	for p.isValidSlot(slot) {
		val, err := p.tx.Int32(p.blockID, p.offset(slot))
		if err != nil {
			return -1, false, fmt.Errorf("could not read int32: %w", err)
		}
		if schema.Flag(val) == flag {
			return slot, true, nil
		}
		slot++
	}
	return -1, false, nil
}

func (p *Page) setFlag(slot int32, flag schema.Flag) error {
	if err := p.tx.SetInt32(p.blockID, p.offset(slot), int32(flag), true); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

type TableScan struct {
	tx          *transaction.Transaction
	layout      *Layout
	rp          *Page
	filename    string
	currentSlot int32
}

var _ query.UpdateScan = (*TableScan)(nil)

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
	rp, err := NewPage(ts.tx, blockID, ts.layout)
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
	rp, err := NewPage(ts.tx, blockID, ts.layout)
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

func (ts *TableScan) BeforeFirst() error {
	if err := ts.moveToBlock(0); err != nil {
		return fmt.Errorf("could not move to block: %w", err)
	}
	return nil
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

func (ts *TableScan) Int32(fieldName schema.FieldName) (int32, error) {
	r, err := ts.rp.Int32(ts.currentSlot, fieldName)
	if err != nil {
		return 0, fmt.Errorf("could not read int32: %w", err)
	}
	return r, nil
}

func (ts *TableScan) Str(fieldName schema.FieldName) (string, error) {
	r, err := ts.rp.Str(ts.currentSlot, fieldName)
	if err != nil {
		return "", fmt.Errorf("could not read string: %w", err)
	}
	return r, nil
}

func (ts *TableScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	switch ts.layout.Schema().Typ(fieldName) {
	case schema.Integer32:
		v, err := ts.Int32(fieldName)
		if err != nil {
			return nil, fmt.Errorf("could not read int32: %w", err)
		}
		return schema.ConstantInt32(v), nil
	case schema.Varchar:
		v, err := ts.Str(fieldName)
		if err != nil {
			return nil, fmt.Errorf("could not read string: %w", err)
		}
		return schema.ConstantStr(v), nil
	}
	return nil, fmt.Errorf("unknown type")
}

func (ts *TableScan) HasField(fieldName schema.FieldName) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) SetInt32(fieldName schema.FieldName, val int32) error {
	if err := ts.rp.SetInt32(ts.currentSlot, fieldName, val); err != nil {
		return fmt.Errorf("could not set int32: %w", err)
	}
	return nil
}

func (ts *TableScan) SetStr(fieldName schema.FieldName, val string) error {
	if err := ts.rp.SetStr(ts.currentSlot, fieldName, val); err != nil {
		return fmt.Errorf("could not set string: %w", err)
	}
	return nil
}

func (ts *TableScan) SetVal(fieldName schema.FieldName, val schema.Constant) error {
	switch v := val.(type) {
	case schema.ConstantInt32:
		return ts.SetInt32(fieldName, int32(v))
	case schema.ConstantStr:
		return ts.SetStr(fieldName, string(v))
	}
	return fmt.Errorf("unknown type")
}

func (ts *TableScan) Insert() error {
	cs, ok, err := ts.rp.InsertAfter(ts.currentSlot)
	if err != nil {
		return fmt.Errorf("could not insert after: %w", err)
	}
	for !ok {
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

func (ts *TableScan) MoveToRID(rid schema.RID) error {
	if err := ts.Close(); err != nil {
		return fmt.Errorf("could not close: %w", err)
	}
	blockID := file.NewBlockID(ts.filename, rid.BlockNum())
	rp, err := NewPage(ts.tx, blockID, ts.layout)
	if err != nil {
		return fmt.Errorf("could not create new record page: %w", err)
	}
	ts.rp = rp
	ts.currentSlot = rid.Slot()
	return nil
}

func (ts *TableScan) RID() schema.RID {
	return schema.NewRID(ts.rp.blockID.Num(), ts.currentSlot)
}
