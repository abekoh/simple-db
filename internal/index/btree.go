package index

import (
	"fmt"
	"math"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type BTreeIndex struct {
	tx                    *transaction.Transaction
	dirLayout, leafLayout *record.Layout
	leafTableName         string
	rootBlockID           file.BlockID
	leaf                  *BTreeLeaf
}

func NewBTreeIndex(tx *transaction.Transaction, idxName string, leafLayout *record.Layout) (Index, error) {
	leafTableName := idxName + "_leaf"
	leafTableSize, err := tx.Size(leafTableName)
	if err != nil {
		return nil, err
	}
	if leafTableSize == 0 {
		blockID, err := tx.Append(leafTableName)
		if err != nil {
			return nil, fmt.Errorf("tx.Append error: %w", err)
		}
		node, err := NewBTreePage(tx, blockID, leafLayout)
		if err != nil {
			return nil, fmt.Errorf("NewBTreePage error: %w", err)
		}
		if err := node.Format(blockID, -1); err != nil {
			return nil, fmt.Errorf("node.Format error: %w", err)
		}
	}

	dirSche := schema.NewSchema()
	dirSche.Add(blockFld, *leafLayout.Schema())
	dirSche.Add(dataFld, *leafLayout.Schema())
	dirLayout := record.NewLayoutSchema(dirSche)

	dirTableName := idxName + "_dir"
	rootBlockID := file.NewBlockID(dirTableName, 0)

	dirTableSize, err := tx.Size(dirTableName)
	if err != nil {
		return nil, fmt.Errorf("tx.Size error: %w", err)
	}
	if dirTableSize == 0 {
		if _, err := tx.Append(dirTableName); err != nil {
			return nil, fmt.Errorf("tx.Append error: %w", err)
		}
		node, err := NewBTreePage(tx, rootBlockID, dirLayout)
		if err != nil {
			return nil, fmt.Errorf("NewBTreePage error: %w", err)
		}
		if err := node.Format(rootBlockID, 0); err != nil {
			return nil, fmt.Errorf("node.Format error: %w", err)
		}
		var minval schema.Constant
		switch dirSche.Typ(dataFld) {
		case schema.Integer32:
			minval = schema.ConstantInt32(math.MinInt32)
		case schema.Varchar:
			minval = schema.ConstantStr("")
		default:
			return nil, fmt.Errorf("unsupported type: %v", dirSche.Typ(dataFld))
		}
		if err := node.InsertDir(0, minval, 0); err != nil {
			return nil, fmt.Errorf("node.InsertDir error: %w", err)
		}
		if err := node.Close(); err != nil {
			return nil, fmt.Errorf("node.Close error: %w", err)
		}
	}
	return &BTreeIndex{
		tx:            tx,
		dirLayout:     dirLayout,
		leafLayout:    leafLayout,
		leafTableName: leafTableName,
		rootBlockID:   rootBlockID,
	}, nil
}

var _ Index = (*BTreeIndex)(nil)

func (bti BTreeIndex) BeforeFirst(searchKey schema.Constant) error {
	//TODO implement me
	panic("implement me")
}

func (bti BTreeIndex) Next() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (bti BTreeIndex) DataRID() (schema.RID, error) {
	//TODO implement me
	panic("implement me")
}

func (bti BTreeIndex) Insert(dataVal schema.Constant, dataRID schema.RID) error {
	//TODO implement me
	panic("implement me")
}

func (bti BTreeIndex) Delete(dataVal schema.Constant, dataRID schema.RID) error {
	//TODO implement me
	panic("implement me")
}

func (bti BTreeIndex) Close() error {
	if bti.leaf != nil {
		if err := bti.leaf.Close(); err != nil {
			return fmt.Errorf("leaf.Close error: %w", err)
		}
	}
}

type BTreePage struct {
	tx             *transaction.Transaction
	currentBlockID file.BlockID
	layout         *record.Layout
}

func NewBTreePage(tx *transaction.Transaction, currentBlockID file.BlockID, layout *record.Layout) (*BTreePage, error) {
	if _, err := tx.Pin(currentBlockID); err != nil {
		return nil, fmt.Errorf("tx.Pin error: %w", err)
	}
	return &BTreePage{
		tx:             tx,
		currentBlockID: currentBlockID,
		layout:         layout,
	}, nil
}

func (btp *BTreePage) Format(blockId file.BlockID, flag int32) error {
	if err := btp.tx.SetInt32(blockId, 0, flag, false); err != nil {
		return fmt.Errorf("tx.SetInt32 error: %w", err)
	}
	if err := btp.tx.SetInt32(blockId, 4, 0, false); err != nil {
		return fmt.Errorf("tx.SetInt32 error: %w", err)
	}
	recSize := btp.layout.SlotSize()
	for pos := int32(2 * 4); pos+recSize <= btp.tx.BlockSize(); pos += recSize {
		for _, fieldName := range btp.layout.Schema().FieldNames() {
			offset, ok := btp.layout.Offset(fieldName)
			if !ok {
				return fmt.Errorf("no such field: %s", fieldName)
			}
			switch btp.layout.Schema().Typ(fieldName) {
			case schema.Integer32:
				if err := btp.tx.SetInt32(blockId, pos+offset, 0, false); err != nil {
					return fmt.Errorf("tx.SetInt32 error: %w", err)
				}
			case schema.Varchar:
				if err := btp.tx.SetStr(blockId, pos+offset, "", false); err != nil {
					return fmt.Errorf("tx.SetString error: %w", err)
				}
			}
		}
	}
	return nil
}

func (btp *BTreePage) InsertDir(slot int32, val schema.Constant, blockNum int32) error {
	if err := btp.insert(slot); err != nil {
		return fmt.Errorf("btp.insert error: %w", err)
	}
	if err := btp.setValue(slot, dataFld, val); err != nil {
		return fmt.Errorf("btp.setValue error: %w", err)
	}
	if err := btp.setValue(slot, blockFld, schema.ConstantInt32(blockNum)); err != nil {
		return fmt.Errorf("btp.setValue error: %w", err)
	}
	return nil
}

func (btp *BTreePage) insert(slot int32) error {
	recsNum, err := btp.recordsNum()
	if err != nil {
		return fmt.Errorf("btp.recordsNum error: %w", err)
	}
	sche := btp.layout.Schema()
	for i := recsNum; i > slot; i-- {
		from := i - 1
		to := i
		for _, fieldName := range sche.FieldNames() {
			fromVal, err := btp.value(from, fieldName)
			if err != nil {
				return fmt.Errorf("btp.value error: %w", err)
			}
			if err := btp.setValue(to, fieldName, fromVal); err != nil {
				return fmt.Errorf("btp.setValue error: %w", err)
			}
		}
	}
	if err := btp.setRecordsNum(recsNum + 1); err != nil {
		return fmt.Errorf("btp.setRecordsNum error: %w", err)
	}
	return nil
}

func (btp *BTreePage) flag() (int32, error) {
	n, err := btp.tx.Int32(btp.currentBlockID, 0)
	if err != nil {
		return 0, fmt.Errorf("tx.Int32 error: %w", err)
	}
	return n, nil
}

func (btp *BTreePage) setFlag(n int32) error {
	if err := btp.tx.SetInt32(btp.currentBlockID, 0, int32(n), true); err != nil {
		return fmt.Errorf("tx.SetInt32 error: %w", err)
	}
	return nil
}

func (btp *BTreePage) recordsNum() (int32, error) {
	n, err := btp.tx.Int32(btp.currentBlockID, 4)
	if err != nil {
		return 0, fmt.Errorf("tx.Int32 error: %w", err)
	}
	return n, nil
}

func (btp *BTreePage) setRecordsNum(n int32) error {
	if err := btp.tx.SetInt32(btp.currentBlockID, 4, int32(n), true); err != nil {
		return fmt.Errorf("tx.SetInt32 error: %w", err)
	}
	return nil
}

func (btp *BTreePage) value(slot int32, fieldName schema.FieldName) (schema.Constant, error) {
	typ := btp.layout.Schema().Typ(fieldName)
	fieldPos, err := btp.fieldPos(slot, fieldName)
	if err != nil {
		return nil, fmt.Errorf("btp.fieldPos error: %w", err)
	}
	switch typ {
	case schema.Integer32:
		v, err := btp.tx.Int32(btp.currentBlockID, fieldPos)
		if err != nil {
			return nil, fmt.Errorf("tx.Int32 error: %w", err)
		}
		return schema.ConstantInt32(v), nil
	case schema.Varchar:
		v, err := btp.tx.Str(btp.currentBlockID, fieldPos)
		if err != nil {
			return nil, fmt.Errorf("tx.Str error: %w", err)
		}
		return schema.ConstantStr(v), nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", typ)
	}
}

func (btp *BTreePage) setValue(slot int32, fieldName schema.FieldName, val schema.Constant) error {
	fieldPos, err := btp.fieldPos(slot, fieldName)
	if err != nil {
		return fmt.Errorf("btp.fieldPos error: %w", err)
	}
	fieldType := btp.layout.Schema().Typ(fieldName)
	switch v := val.(type) {
	case schema.ConstantInt32:
		if fieldType != schema.Integer32 {
			return fmt.Errorf("field type mismatch: %v", fieldType)
		}
		if err := btp.tx.SetInt32(btp.currentBlockID, fieldPos, int32(v), true); err != nil {
			return fmt.Errorf("tx.SetInt32 error: %w", err)
		}
	case schema.ConstantStr:
		if fieldType != schema.Varchar {
			return fmt.Errorf("field type mismatch: %v", fieldType)
		}
		if err := btp.tx.SetStr(btp.currentBlockID, fieldPos, string(v), true); err != nil {
			return fmt.Errorf("tx.SetStr error: %w", err)
		}
	}
	return nil
}

func (btp *BTreePage) fieldPos(slot int32, fieldName schema.FieldName) (int32, error) {
	offset, ok := btp.layout.Offset(fieldName)
	if !ok {
		return -1, fmt.Errorf("no such field: %s", fieldName)
	}
	slotPos := 4*2 + (slot * btp.layout.SlotSize())
	return slotPos + offset, nil
}

func (btp *BTreePage) FindSlotBefore(searchKey schema.Constant) (int32, error) {
	var slot int32
	for {
		recsNum, err := btp.recordsNum()
		if err != nil {
			return -1, fmt.Errorf("btp.recordsNum error: %w", err)
		}
		if slot >= recsNum {
			return slot - 1, nil
		}
		val, err := btp.value(slot, dataFld)
		if err != nil {
			return -1, fmt.Errorf("btp.value error: %w", err)
		}
		if searchKey.Compare(val) >= 0 {
			return slot - 1, nil
		}
		slot++
	}
}

func (btp *BTreePage) Close() error {
	if err := btp.tx.Unpin(btp.currentBlockID); err != nil {
		return fmt.Errorf("tx.Unpin error: %w", err)
	}
	return nil
}

func (btp *BTreePage) IsFull() (bool, error) {
	recsNum, err := btp.recordsNum()
	if err != nil {
		return false, fmt.Errorf("btp.recordsNum error: %w", err)
	}
	slotSize := btp.layout.SlotSize()
	slotPos := 4*2 + (recsNum + 1*slotSize)
	return slotPos >= btp.tx.BlockSize(), nil
}

func (btp *BTreePage) Split(splitPos int32, flag int32) (file.BlockID, error) {
	newBlockId, err := btp.tx.Append(btp.currentBlockID.Filename())
	if err != nil {
		return file.BlockID{}, fmt.Errorf("tx.Append error: %w", err)
	}
	newPage, err := NewBTreePage(btp.tx, newBlockId, btp.layout)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("NewBTreePage error: %w", err)
	}

	sche := btp.layout.Schema()
	destSlot := int32(0)
	for {
		recsNum, err := btp.recordsNum()
		if err != nil {
			return file.BlockID{}, fmt.Errorf("btp.recordsNum error: %w", err)
		}
		if destSlot >= recsNum {
			break
		}
		if err := newPage.insert(destSlot); err != nil {
			return file.BlockID{}, fmt.Errorf("newPage.insert error: %w", err)
		}
		for _, fieldName := range sche.FieldNames() {
			val, err := btp.value(destSlot, fieldName)
			if err != nil {
				return file.BlockID{}, fmt.Errorf("btp.value error: %w", err)
			}
			if err := newPage.setValue(destSlot, fieldName, val); err != nil {
				return file.BlockID{}, fmt.Errorf("newPage.setValue error: %w", err)
			}
		}
	}
	if err := newPage.setFlag(flag); err != nil {
		return file.BlockID{}, fmt.Errorf("newPage.setFlag error: %w", err)
	}
	if err := newPage.Close(); err != nil {
		return file.BlockID{}, fmt.Errorf("newPage.Close error: %w", err)
	}
	return newBlockId, nil
}

func (btp *BTreePage) AppendNew(flag int32) error {
	blockID, err := btp.tx.Append(btp.currentBlockID.Filename())
	if err != nil {
		return fmt.Errorf("tx.Append error: %w", err)
	}
	if err := btp.Format(blockID, flag); err != nil {
		return fmt.Errorf("btp.Format error: %w", err)
	}
	return nil
}

func (btp *BTreePage) ChildNum() (int32, error) {
	v, err := btp.value(0, blockFld)
	if err != nil {
		return 0, fmt.Errorf("btp.value error: %w", err)
	}
	if v, ok := v.(schema.ConstantInt32); ok {
		return int32(v), nil
	}
	return 0, fmt.Errorf("type mismatch")
}

func (btp *BTreePage) DataRID(slot int32) (schema.RID, error) {
	blockNum, err := btp.value(slot, blockFld)
	if err != nil {
		return schema.RID{}, fmt.Errorf("btp.value error: %w", err)
	}
	id, err := btp.value(slot, idFld)
	if err != nil {
		return schema.RID{}, fmt.Errorf("btp.value error: %w", err)
	}
	if blockNum, ok := blockNum.(schema.ConstantInt32); ok {
		if id, ok := id.(schema.ConstantInt32); ok {
			return schema.NewRID(int32(blockNum), int32(id)), nil
		}
	}
	return schema.RID{}, fmt.Errorf("type mismatch")
}

func (btp *BTreePage) InsertLeaf(slot int32, val schema.Constant, rid schema.RID) error {
	if err := btp.insert(slot); err != nil {
		return fmt.Errorf("btp.insert error: %w", err)
	}
	if err := btp.setValue(slot, dataFld, val); err != nil {
		return fmt.Errorf("btp.setValue error: %w", err)
	}
	if err := btp.setValue(slot, blockFld, schema.ConstantInt32(rid.BlockNum())); err != nil {
		return fmt.Errorf("btp.setValue error: %w", err)
	}
	if err := btp.setValue(slot, idFld, schema.ConstantInt32(rid.Slot())); err != nil {
		return fmt.Errorf("btp.setValue error: %w", err)
	}
	return nil
}

func (btp *BTreePage) Delete(slot int32) error {
	recsNum, err := btp.recordsNum()
	if err != nil {
		return fmt.Errorf("btp.recordsNum error: %w", err)
	}
	for i := slot + 1; i < recsNum; i++ {
		from := i
		to := i - 1
		for _, fieldName := range btp.layout.Schema().FieldNames() {
			val, err := btp.value(from, fieldName)
			if err != nil {
				return fmt.Errorf("btp.value error: %w", err)
			}
			if err := btp.setValue(to, fieldName, val); err != nil {
				return fmt.Errorf("btp.setValue error: %w", err)
			}
		}
	}
	if err := btp.setRecordsNum(recsNum - 1); err != nil {
		return fmt.Errorf("btp.setRecordsNum error: %w", err)
	}
	return nil
}

type BTreeLeaf struct {
	tx          *transaction.Transaction
	layout      *record.Layout
	searchKey   schema.Constant
	contents    *BTreePage
	currentSlot int32
	filename    string
}

func NewBTreeLeaf(tx *transaction.Transaction, blockID file.BlockID, layout *record.Layout, searchKey schema.Constant) (*BTreeLeaf, error) {
	contents, err := NewBTreePage(tx, blockID, layout)
	if err != nil {
		return nil, fmt.Errorf("NewBTreePage error: %w", err)
	}
	currentSlot, err := contents.FindSlotBefore(searchKey)
	if err != nil {
		return nil, fmt.Errorf("contents.FindSlotBefore error: %w", err)
	}
	return &BTreeLeaf{
		tx:          tx,
		layout:      layout,
		searchKey:   searchKey,
		contents:    contents,
		currentSlot: currentSlot,
		filename:    blockID.Filename(),
	}, nil
}

func (btl *BTreeLeaf) Close(searchKey schema.Constant) error {
	if err := btl.contents.Close(); err != nil {
		return fmt.Errorf("contents.Close error: %w", err)
	}
	return nil
}

func (btl *BTreeLeaf) Next() (bool, error) {
}
