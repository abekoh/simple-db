package multibuffer

import (
	"fmt"
	"math"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

func bestRoot(available, size int32) int32 {
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	var k int32 = math.MaxInt32
	i := 1.0
	for k > avail {
		i += 1.0
		k = int32(math.Ceil(math.Pow(float64(size), 1.0/i)))
	}
	return k
}

func bestFactor(available, size int32) int32 {
	avail := available - 2
	if avail <= 1 {
		return 1
	}
	k := size
	i := 1.0
	for k > avail {
		i += 1.0
		k = int32(math.Ceil(float64(size) / i))
	}
	return k
}

type ChunkScan struct {
	buffers                         []record.Page
	tx                              *transaction.Transaction
	filename                        string
	layout                          *record.Layout
	startBNum, endBNum, currentBNum int32
	rp                              *record.Page
	currentSlot                     int32
}

var _ query.Scan = (*ChunkScan)(nil)

func NewChunkScan(
	tx *transaction.Transaction,
	filename string,
	layout *record.Layout,
	startBNum, endBNum int32,
) (*ChunkScan, error) {
	buffs := make([]record.Page, 0, endBNum-startBNum+1)
	for i := startBNum; i <= endBNum; i++ {
		blockID := file.NewBlockID(filename, i)
		rp, err := record.NewPage(tx, blockID, layout)
		if err != nil {
			return nil, fmt.Errorf("record.NewPage error: %w", err)
		}
		buffs = append(buffs, *rp)
	}
	cs := ChunkScan{
		buffers:   buffs,
		tx:        tx,
		filename:  filename,
		layout:    layout,
		startBNum: startBNum,
		endBNum:   endBNum,
	}
	cs.moveToBlock(startBNum)
	return &cs, nil
}

func (c *ChunkScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	switch c.layout.Schema().Typ(fieldName) {
	case schema.Integer32:
		val, err := c.Int32(fieldName)
		if err != nil {
			return nil, fmt.Errorf("c.Int32 error: %w", err)
		}
		return schema.ConstantInt32(val), nil
	case schema.Varchar:
		val, err := c.Str(fieldName)
		if err != nil {
			return nil, fmt.Errorf("c.Str error: %w", err)
		}
		return schema.ConstantStr(val), nil
	default:
		return nil, fmt.Errorf("unexpected field type: %v", c.layout.Schema().Typ(fieldName))
	}
}

func (c *ChunkScan) BeforeFirst() error {
	c.moveToBlock(c.startBNum)
	return nil
}

func (c *ChunkScan) Next() (bool, error) {
	cs, ok, err := c.rp.NextAfter(c.currentSlot)
	if err != nil {
		return false, fmt.Errorf("rp.NextAfter error: %w", err)
	}
	c.currentSlot = cs
	for ok {
		if c.currentBNum == c.endBNum {
			return false, nil
		}
		c.moveToBlock(c.rp.BlockID().Num() + 1)
		c.currentSlot, ok, err = c.rp.NextAfter(c.currentSlot)
		if err != nil {
			return false, fmt.Errorf("rp.NextAfter error: %w", err)
		}
	}
	return true, nil
}

func (c *ChunkScan) Int32(fieldName schema.FieldName) (int32, error) {
	val, err := c.rp.Int32(c.currentSlot, fieldName)
	if err != nil {
		return 0, fmt.Errorf("rp.Int32 error: %w", err)
	}
	return val, nil
}

func (c *ChunkScan) Str(fieldName schema.FieldName) (string, error) {
	val, err := c.rp.Str(c.currentSlot, fieldName)
	if err != nil {
		return "", fmt.Errorf("rp.Str error: %w", err)
	}
	return val, nil
}

func (c *ChunkScan) HasField(fieldName schema.FieldName) bool {
	return c.layout.Schema().HasField(fieldName)
}

func (c *ChunkScan) Close() error {
	for i := 0; i < len(c.buffers); i++ {
		blockID := file.NewBlockID(c.filename, c.startBNum+int32(i))
		if err := c.tx.Unpin(blockID); err != nil {
			return fmt.Errorf("tx.Unpin error: %w", err)
		}
	}
	return nil
}

func (c *ChunkScan) moveToBlock(blockNum int32) {
	c.currentBNum = blockNum
	c.rp = &c.buffers[blockNum-c.startBNum]
	c.currentSlot = -1
}

type ProductScan struct {
	tx                                *transaction.Transaction
	lhsScan, rhsScan, prodScan        query.Scan
	filename                          string
	layout                            *record.Layout
	chunkSize, nextBlockNum, fileSize int32
}

var _ query.Scan = (*ProductScan)(nil)

func NewProductScan(
	tx *transaction.Transaction,
	lshScan query.Scan,
	tableName string,
	layout *record.Layout,
) (*ProductScan, error) {
	filename := fmt.Sprintf("%s.tbl", tableName)
	fileSize, err := tx.Size(filename)
	if err != nil {
		return nil, fmt.Errorf("tx.Size error: %w", err)
	}
	chunkSize := bestFactor(int32(tx.AvailableBuffersNum()), fileSize)
	ps := ProductScan{
		tx:        tx,
		lhsScan:   lshScan,
		filename:  filename,
		layout:    layout,
		chunkSize: chunkSize,
	}
	if err := ps.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("ps.BeforeFirst error: %w", err)
	}
	return &ps, nil
}

func (p *ProductScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	//TODO implement me
	panic("implement me")
}

func (p *ProductScan) BeforeFirst() error {
	p.nextBlockNum = 0
	if _, err := p.useNextChunk(); err != nil {
		return fmt.Errorf("p.useNextChunk error: %w", err)
	}
	return nil
}

func (p *ProductScan) Next() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (p *ProductScan) Int32(fieldName schema.FieldName) (int32, error) {
	//TODO implement me
	panic("implement me")
}

func (p *ProductScan) Str(fieldName schema.FieldName) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (p *ProductScan) HasField(fieldName schema.FieldName) bool {
	//TODO implement me
	panic("implement me")
}

func (p *ProductScan) Close() error {
	//TODO implement me
	panic("implement me")
}

func (p *ProductScan) useNextChunk() (bool, error) {
	if p.nextBlockNum >= p.fileSize {
		return false, nil
	}
	if p.rhsScan != nil {
		if err := p.rhsScan.Close(); err != nil {
			return false, fmt.Errorf("p.rhsScan.Close error: %w", err)
		}
	}
	end := p.nextBlockNum + p.chunkSize - 1
	if end >= p.fileSize {
		end = p.fileSize - 1
	}
	rhsScan, err := NewChunkScan(p.tx, p.filename, p.layout, p.nextBlockNum, end)
	if err != nil {
		return false, fmt.Errorf("NewChunkScan error: %w", err)
	}
	if err := p.lhsScan.BeforeFirst(); err != nil {
		return false, fmt.Errorf("p.lhsScan.BeforeFirst error: %w", err)
	}
	prodScan, err := query.NewProductScan(p.lhsScan, rhsScan)
	if err != nil {
		return false, fmt.Errorf("query.NewProductScan error: %w", err)
	}
	p.prodScan = prodScan
	p.nextBlockNum = end + 1
	return true, nil
}
