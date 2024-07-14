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
	if err := bti.Close(); err != nil {
		return fmt.Errorf("bti.Close error: %w", err)
	}
	root, err := NewBTreeDir(bti.tx, bti.rootBlockID, bti.dirLayout)
	if err != nil {
		return fmt.Errorf("NewBTreeDir error: %w", err)
	}
	blockNum, err := root.Search(searchKey)
	if err != nil {
		return fmt.Errorf("root.Search error: %w", err)
	}
	if err := root.Close(); err != nil {
		return fmt.Errorf("root.Close error: %w", err)
	}
	leafBlockID := file.NewBlockID(bti.leafTableName, blockNum)
	leaf, err := NewBTreeLeaf(bti.tx, leafBlockID, bti.leafLayout, searchKey)
	if err != nil {
		return fmt.Errorf("NewBTreeLeaf error: %w", err)
	}
	bti.leaf = leaf
	return nil
}

func (bti BTreeIndex) Next() (bool, error) {
	if bti.leaf == nil {
		return false, fmt.Errorf("bti.leaf is nil")
	}
	if ok, err := bti.leaf.Next(); err != nil {
		return false, fmt.Errorf("leaf.Next error: %w", err)
	} else {
		return ok, nil
	}
}

func (bti BTreeIndex) DataRID() (schema.RID, error) {
	if bti.leaf == nil {
		return schema.RID{}, fmt.Errorf("bti.leaf is nil")
	}
	rid, err := bti.leaf.DataRID()
	if err != nil {
		return schema.RID{}, fmt.Errorf("leaf.DataRID error: %w", err)
	}
	return rid, nil
}

func (bti BTreeIndex) Insert(dataVal schema.Constant, dataRID schema.RID) error {
	if bti.leaf == nil {
		return fmt.Errorf("bti.leaf is nil")
	}
	if err := bti.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("bti.BeforeFirst error: %w", err)
	}
	e, err := bti.leaf.Insert(dataRID)
	if err != nil {
		return fmt.Errorf("leaf.Insert error: %w", err)
	}
	if e != nil {
		return nil
	}
	root, err := NewBTreeDir(bti.tx, bti.rootBlockID, bti.dirLayout)
	if err != nil {
		return fmt.Errorf("NewBTreeDir error: %w", err)
	}
	e2, err := root.Insert(DirEntry{dataValue: dataVal, blockNum: e.blockNum})
	if err != nil {
		return fmt.Errorf("root.Insert error: %w", err)
	}
	if e2 != nil {
		if err := root.MakeNewRoot(*e2); err != nil {
			return fmt.Errorf("root.MakeNewRoot error: %w", err)
		}
	}
	if err := root.Close(); err != nil {
		return fmt.Errorf("root.Close error: %w", err)
	}
	return nil
}

func (bti BTreeIndex) Delete(dataVal schema.Constant, dataRID schema.RID) error {
	if bti.leaf == nil {
		return fmt.Errorf("bti.leaf is nil")
	}
	if err := bti.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("bti.BeforeFirst error: %w", err)
	}
	if err := bti.leaf.Delete(dataRID); err != nil {
		return fmt.Errorf("leaf.Delete error: %w", err)
	}
	if err := bti.leaf.Close(); err != nil {
		return fmt.Errorf("leaf.Close error: %w", err)
	}
	return nil
}

func (bti BTreeIndex) Close() error {
	if bti.leaf != nil {
		if err := bti.leaf.Close(); err != nil {
			return fmt.Errorf("leaf.Close error: %w", err)
		}
	}
	return nil
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

type BTreeDir struct {
	tx       *transaction.Transaction
	layout   *record.Layout
	contents *BTreePage
	filename string
}

func NewBTreeDir(tx *transaction.Transaction, blockID file.BlockID, layout *record.Layout) (*BTreeDir, error) {
	contents, err := NewBTreePage(tx, blockID, layout)
	if err != nil {
		return nil, fmt.Errorf("NewBTreePage error: %w", err)
	}
	return &BTreeDir{
		tx:       tx,
		layout:   layout,
		contents: contents,
		filename: blockID.Filename(),
	}, nil
}

func (btd *BTreeDir) Close() error {
	if err := btd.contents.Close(); err != nil {
		return fmt.Errorf("contents.Close error: %w", err)
	}
	return nil
}

func (btd *BTreeDir) Search(searchKey schema.Constant) (int32, error) {
	childBlock, err := btd.findChildBlock(searchKey)
	if err != nil {
		return -1, fmt.Errorf("btd.findChildBlock error: %w", err)
	}
	for {
		flg, err := btd.contents.flag()
		if err != nil {
			return -1, fmt.Errorf("btd.contents.flag error: %w", err)
		}
		if flg <= 0 {
			break
		}
		if err := btd.contents.Close(); err != nil {
			return -1, fmt.Errorf("btd.contents.Close error: %w", err)
		}
		cs, err := NewBTreePage(btd.tx, childBlock, btd.layout)
		if err != nil {
			return -1, fmt.Errorf("NewBTreePage error: %w", err)
		}
		btd.contents = cs
		childBlock, err = btd.findChildBlock(searchKey)
		if err != nil {
			return -1, fmt.Errorf("btd.findChildBlock error: %w", err)
		}
	}
	return childBlock.Num(), nil
}

func (btd *BTreeDir) MakeNewRoot(e DirEntry) error {
	firstVal, err := btd.contents.value(0, dataFld)
	if err != nil {
		return fmt.Errorf("btd.contents.value error: %w", err)
	}
	level, err := btd.contents.flag()
	if err != nil {
		return fmt.Errorf("btd.contents.flag error: %w", err)
	}
	newBlockID, err := btd.contents.Split(0, level)
	if err != nil {
		return fmt.Errorf("btd.contents.Split error: %w", err)
	}
	oldRoot := DirEntry{dataValue: firstVal, blockNum: newBlockID.Num()}
	if _, err := btd.InsertEntry(oldRoot); err != nil {
		return fmt.Errorf("btd.InsertEntry error: %w", err)
	}
	if _, err := btd.InsertEntry(e); err != nil {
		return fmt.Errorf("btd.InsertEntry error: %w", err)
	}
	if err := btd.contents.setFlag(level + 1); err != nil {
		return fmt.Errorf("btd.contents.setFlag error: %w", err)
	}
	return nil
}

func (btd *BTreeDir) Insert(e DirEntry) (*DirEntry, error) {
	flg, err := btd.contents.flag()
	if err != nil {
		return nil, fmt.Errorf("btd.contents.flag error: %w", err)
	}
	if flg == 0 {
		ne, err := btd.InsertEntry(e)
		if err != nil {
			return nil, fmt.Errorf("btd.InsertEntry error: %w", err)
		}
		return ne, nil
	}
	childBlockID, err := btd.findChildBlock(e.dataValue)
	if err != nil {
		return nil, fmt.Errorf("btd.findChildBlock error: %w", err)
	}
	child, err := NewBTreeDir(btd.tx, childBlockID, btd.layout)
	if err != nil {
		return nil, fmt.Errorf("NewBTreeDir error: %w", err)
	}
	myEntry, err := child.Insert(e)
	if err != nil {
		return nil, fmt.Errorf("child.Insert error: %w", err)
	}
	if myEntry != nil {
		ne, err := btd.InsertEntry(e)
		if err != nil {
			return nil, fmt.Errorf("btd.InsertEntry error: %w", err)
		}
		return ne, nil
	} else {
		return nil, nil
	}
}

func (btd *BTreeDir) InsertEntry(e DirEntry) (*DirEntry, error) {
	slot, err := btd.contents.FindSlotBefore(e.dataValue)
	if err != nil {
		return nil, fmt.Errorf("btd.contents.FindSlotBefore error: %w", err)
	}
	newSlot := slot + 1
	if err := btd.contents.InsertDir(newSlot, e.dataValue, e.blockNum); err != nil {
		return nil, fmt.Errorf("btd.contents.InsertDir error: %w", err)
	}
	isFull, err := btd.contents.IsFull()
	if err != nil {
		return nil, fmt.Errorf("btd.contents.IsFull error: %w", err)
	}
	if !isFull {
		return nil, nil
	}

	// split
	level, err := btd.contents.flag()
	if err != nil {
		return nil, fmt.Errorf("btd.contents.flag error: %w", err)
	}
	recsNum, err := btd.contents.recordsNum()
	if err != nil {
		return nil, fmt.Errorf("btd.contents.recordsNum error: %w", err)
	}
	splitPos := recsNum / 2
	splitVal, err := btd.contents.value(splitPos, dataFld)
	if err != nil {
		return nil, fmt.Errorf("btd.contents.value error: %w", err)
	}
	newBlockID, err := btd.contents.Split(splitPos, level)
	if err != nil {
		return nil, fmt.Errorf("btd.contents.Split error: %w", err)
	}
	return &DirEntry{dataValue: splitVal, blockNum: newBlockID.Num()}, nil
}

func (btd *BTreeDir) findChildBlock(searchKey schema.Constant) (file.BlockID, error) {
	slot, err := btd.contents.FindSlotBefore(searchKey)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("btd.findChildBlock error: %w", err)
	}
	val, err := btd.contents.value(slot+1, dataFld)
	if err != nil {
		return file.BlockID{}, fmt.Errorf("btd.contents.value error: %w", err)
	}
	if searchKey.Equals(val) {
		slot++
	}
	blockNum, err := btd.contents.ChildNum()
	if err != nil {
		return file.BlockID{}, fmt.Errorf("btd.contents.ChildNum error: %w", err)
	}
	return file.NewBlockID(btd.filename, blockNum), nil
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

func (btl *BTreeLeaf) Close() error {
	if err := btl.contents.Close(); err != nil {
		return fmt.Errorf("contents.Close error: %w", err)
	}
	return nil
}

func (btl *BTreeLeaf) Next() (bool, error) {
	tryOverflow := func() (bool, error) {
		firstKey, err := btl.contents.value(0, dataFld)
		if err != nil {
			return false, fmt.Errorf("contents.value error: %w", err)
		}
		flag, err := btl.contents.flag()
		if err != nil {
			return false, fmt.Errorf("contents.flag error: %w", err)
		}
		if !btl.searchKey.Equals(firstKey) || flag < 0 {
			return false, nil
		}
		if err := btl.contents.Close(); err != nil {
			return false, fmt.Errorf("contents.Close error: %w", err)
		}
		nextBlockID := file.NewBlockID(btl.filename, flag)
		cs, err := NewBTreePage(btl.tx, nextBlockID, btl.layout)
		if err != nil {
			return false, fmt.Errorf("NewBTreePage error: %w", err)
		}
		btl.contents = cs
		btl.currentSlot = 0
		return false, nil
	}

	btl.currentSlot++
	recsNum, err := btl.contents.recordsNum()
	if err != nil {
		return false, fmt.Errorf("contents.recordsNum error: %w", err)
	}
	if btl.currentSlot >= recsNum {
		return tryOverflow()
	}
	val, err := btl.contents.value(btl.currentSlot, dataFld)
	if err != nil {
		return false, fmt.Errorf("contents.value error: %w", err)
	}
	if btl.searchKey.Compare(val) != 0 {
		return tryOverflow()
	}
	return true, nil
}

func (btl *BTreeLeaf) DataRID() (schema.RID, error) {
	return btl.contents.DataRID(btl.currentSlot)
}

func (btl *BTreeLeaf) Delete(rid schema.RID) error {
	for {
		ok, err := btl.Next()
		if err != nil {
			return fmt.Errorf("btl.Next error: %w", err)
		}
		if !ok {
			return nil
		}
		r, err := btl.DataRID()
		if err != nil {
			return fmt.Errorf("btl.DataRID error: %w", err)
		}
		if r.Equals(rid) {
			if err := btl.contents.Delete(btl.currentSlot); err != nil {
				return fmt.Errorf("btl.contents.Delete error: %w", err)
			}
			return nil
		}
	}
}

func (btl *BTreeLeaf) Insert(rid schema.RID) (*DirEntry, error) {
	flg, err := btl.contents.flag()
	if err != nil {
		return nil, fmt.Errorf("btl.contents.flag error: %w", err)
	}
	val, err := btl.contents.value(0, dataFld)
	if err != nil {
		return nil, fmt.Errorf("btl.contents.value error: %w", err)
	}
	if flg >= 0 && val.Compare(btl.searchKey) > 0 {
		newBlockID, err := btl.contents.Split(0, flg)
		if err != nil {
			return nil, fmt.Errorf("btl.contents.Split error: %w", err)
		}
		var currentSlot int32
		if err := btl.contents.setFlag(-1); err != nil {
			return nil, fmt.Errorf("btl.contents.setFlag error: %w", err)
		}
		if err := btl.contents.InsertLeaf(currentSlot, btl.searchKey, rid); err != nil {
			return nil, fmt.Errorf("btl.contents.InsertLeaf error: %w", err)
		}
		return &DirEntry{dataValue: btl.searchKey, blockNum: newBlockID.Num()}, nil
	}

	btl.currentSlot++
	if err := btl.contents.InsertLeaf(btl.currentSlot, btl.searchKey, rid); err != nil {
		return nil, fmt.Errorf("btl.contents.insert error: %w", err)
	}
	isFull, err := btl.contents.IsFull()
	if err != nil {
		return nil, fmt.Errorf("btl.contents.IsFull error: %w", err)
	}
	if !isFull {
		return nil, nil
	}

	// split the leaf page
	firstKey, err := btl.contents.value(0, dataFld)
	if err != nil {
		return nil, fmt.Errorf("btl.contents.value error: %w", err)
	}
	recsNum, err := btl.contents.recordsNum()
	if err != nil {
		return nil, fmt.Errorf("btl.contents.recordsNum error: %w", err)
	}
	lastKey, err := btl.contents.value(recsNum-1, dataFld)
	if err != nil {
		return nil, fmt.Errorf("btl.contents.value error: %w", err)
	}
	if firstKey.Equals(lastKey) {
		newBlockID, err := btl.contents.Split(1, flg)
		if err != nil {
			return nil, fmt.Errorf("btl.contents.Split error: %w", err)
		}
		if err := btl.contents.setFlag(newBlockID.Num()); err != nil {
			return nil, fmt.Errorf("btl.contents.setFlag error: %w", err)
		}
		return nil, nil
	} else {
		splitPos := recsNum / 2
		splitKey, err := btl.contents.value(splitPos, dataFld)
		if err != nil {
			return nil, fmt.Errorf("btl.contents.value error: %w", err)
		}
		if splitKey.Equals(btl.searchKey) {
			var key schema.Constant
			for {
				key, err = btl.contents.value(splitPos, dataFld)
				if err != nil {
					return nil, fmt.Errorf("btl.contents.value error: %w", err)
				}
				if !key.Equals(btl.searchKey) {
					break
				}
				splitPos++
			}
			splitKey = key
		} else {
			for {
				key, err := btl.contents.value(splitPos-1, dataFld)
				if err != nil {
					return nil, fmt.Errorf("btl.contents.value error: %w", err)
				}
				if !key.Equals(btl.searchKey) {
					break
				}
				splitPos--
			}
		}
		newBlockID, err := btl.contents.Split(splitPos, -1)
		if err != nil {
			return nil, fmt.Errorf("btl.contents.Split error: %w", err)
		}
		return &DirEntry{dataValue: splitKey, blockNum: newBlockID.Num()}, nil
	}
}

type DirEntry struct {
	dataValue schema.Constant
	blockNum  int32
}
