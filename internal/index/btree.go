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
	//TODO implement me
	panic("implement me")
}

func (btp BTreePage) InsertDir(slot int32, val schema.Constant, blockNum int32) error {
	//TODO implement me
	panic("implement me")
}

func (btp BTreePage) Close() error {
	//TODO implement me
	panic("implement me")
}
