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
	//TODO implement me
	panic("implement me")
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

func (btp BTreePage) Format(blockId file.BlockID, flag int32) error {
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

func (btp BTreePage) InsertDir(slot int32, val schema.Constant, blockNum int32) error {
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

func (btp BTreePage) insert(slot int32) error {
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

func (btp BTreePage) recordsNum() (int32, error) {
	n, err := btp.tx.Int32(btp.currentBlockID, 4)
	if err != nil {
		return 0, fmt.Errorf("tx.Int32 error: %w", err)
	}
	return n, nil
}

func (btp BTreePage) setRecordsNum(n int32) error {
	if err := btp.tx.SetInt32(btp.currentBlockID, 4, int32(n), true); err != nil {
		return fmt.Errorf("tx.SetInt32 error: %w", err)
	}
	return nil
}

func (btp BTreePage) value(slot int32, fieldName schema.FieldName) (schema.Constant, error) {
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

func (btp BTreePage) setValue(slot int32, fieldName schema.FieldName, val schema.Constant) error {
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

func (btp BTreePage) fieldPos(slot int32, fieldName schema.FieldName) (int32, error) {
	offset, ok := btp.layout.Offset(fieldName)
	if !ok {
		return -1, fmt.Errorf("no such field: %s", fieldName)
	}
	slotPos := 4*2 + (slot * btp.layout.SlotSize())
	return slotPos + offset, nil
}

func (btp BTreePage) Close() error {
	//TODO implement me
	panic("implement me")
}
