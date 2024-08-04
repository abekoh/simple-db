package metadata

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/abekoh/simple-db/internal/index"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

const maxTableNameLength = 16

const (
	tableCatalogTableName = "table_catalog"
	fieldCatalogTableName = "field_catalog"
	viewCatalogTableName  = "view_catalog"
	indexCatalogTableName = "index_catalog"
	tableNameField        = "table_name"
	slotSizeField         = "slot_size"
	fieldNameField        = "field_name"
	typeField             = "type"
	lengthField           = "length"
	offsetField           = "offset"
	indexNameField        = "index_name"
	viewNameField         = "view_name"
	viewDefField          = "view_def"
)

type TableManager struct {
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

func NewTableManager(isNew bool, tx *transaction.Transaction) (*TableManager, error) {
	m := &TableManager{}

	tableCatalogSchema := schema.NewSchema()
	tableCatalogSchema.AddStrField(tableNameField, maxTableNameLength)
	tableCatalogSchema.AddInt32Field(slotSizeField)
	m.tableCatalogLayout = record.NewLayoutSchema(tableCatalogSchema)

	fieldCatalogSchema := schema.NewSchema()
	fieldCatalogSchema.AddStrField(tableNameField, maxTableNameLength)
	fieldCatalogSchema.AddStrField(fieldNameField, maxTableNameLength)
	fieldCatalogSchema.AddInt32Field(typeField)
	fieldCatalogSchema.AddInt32Field(lengthField)
	fieldCatalogSchema.AddInt32Field(offsetField)
	m.fieldCatalogLayout = record.NewLayoutSchema(fieldCatalogSchema)

	if isNew {
		if err := m.CreateTable(tableCatalogTableName, tableCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("create table catalog error: %w", err)
		}
		if err := m.CreateTable(fieldCatalogTableName, fieldCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("create field catalog error: %w", err)
		}
	}

	return m, nil
}

func (m *TableManager) CreateTable(tableName string, schema schema.Schema, tx *transaction.Transaction) error {
	layout := record.NewLayoutSchema(schema)
	tableCatalog, err := record.NewTableScan(tx, tableCatalogTableName, m.tableCatalogLayout)
	if err != nil {
		return fmt.Errorf("table catalog scan error: %w", err)
	}
	if err := tableCatalog.Insert(); err != nil {
		return fmt.Errorf("table catalog insert error: %w", err)
	}
	if err := tableCatalog.SetStr(tableNameField, tableName); err != nil {
		return fmt.Errorf("table catalog set string error: %w", err)
	}
	if err := tableCatalog.SetInt32(slotSizeField, layout.SlotSize()); err != nil {
		return fmt.Errorf("table catalog set int32 error: %w", err)
	}
	if err := tableCatalog.Close(); err != nil {
		return fmt.Errorf("table catalog close error: %w", err)
	}

	fieldCatalog, err := record.NewTableScan(tx, fieldCatalogTableName, m.fieldCatalogLayout)
	if err != nil {
		return fmt.Errorf("field catalog scan error: %w", err)
	}
	for _, fieldName := range schema.FieldNames() {
		if err := fieldCatalog.Insert(); err != nil {
			return fmt.Errorf("field catalog insert error: %w", err)
		}
		if err := fieldCatalog.SetStr(tableNameField, tableName); err != nil {
			return fmt.Errorf("field catalog set string error: %w", err)
		}
		if err := fieldCatalog.SetStr(fieldNameField, string(fieldName)); err != nil {
			return fmt.Errorf("field catalog set string error: %w", err)
		}
		if err := fieldCatalog.SetInt32(typeField, int32(schema.Typ(fieldName))); err != nil {
			return fmt.Errorf("field catalog set int32 error: %w", err)
		}
		if err := fieldCatalog.SetInt32(lengthField, schema.Length(fieldName)); err != nil {
			return fmt.Errorf("field catalog set int32 error: %w", err)
		}
		offset, ok := layout.Offset(fieldName)
		if !ok {
			return fmt.Errorf("offset not found for %s", fieldName)
		}
		if err := fieldCatalog.SetInt32(offsetField, offset); err != nil {
			return fmt.Errorf("field catalog set int32 error: %w", err)
		}
	}
	if err := fieldCatalog.Close(); err != nil {
		return fmt.Errorf("field catalog close error: %w", err)
	}
	return nil
}

func (m *TableManager) Layout(tableName string, tx *transaction.Transaction) (*record.Layout, error) {
	var size int32 = -1
	tableCatalogScan, err := record.NewTableScan(tx, tableCatalogTableName, m.tableCatalogLayout)
	if err != nil {
		return nil, fmt.Errorf("table catalog scan error: %w", err)
	}
	for {
		ok, err := tableCatalogScan.Next()
		if err != nil {
			return nil, fmt.Errorf("table catalog next error: %w", err)
		}
		if !ok {
			break
		}
		if name, err := tableCatalogScan.Str(tableNameField); err != nil {
			return nil, fmt.Errorf("table catalog get string error: %w", err)
		} else if name == tableName {
			s, err := tableCatalogScan.Int32(slotSizeField)
			if err != nil {
				return nil, fmt.Errorf("table catalog get int32 error: %w", err)
			}
			size = s
			break
		}
	}
	if err := tableCatalogScan.Close(); err != nil {
		return nil, fmt.Errorf("table catalog close error: %w", err)
	}

	sche := schema.NewSchema()
	offsets := make(map[schema.FieldName]int32)
	fieldCatalog, err := record.NewTableScan(tx, fieldCatalogTableName, m.fieldCatalogLayout)
	if err != nil {
		return nil, fmt.Errorf("field catalog scan error: %w", err)
	}
	for {
		if ok, err := fieldCatalog.Next(); err != nil {
			return nil, fmt.Errorf("field catalog next error: %w", err)
		} else if !ok {
			break
		}
		if name, err := fieldCatalog.Str(tableNameField); err != nil {
			return nil, fmt.Errorf("field catalog get string error: %w", err)
		} else if name == tableName {
			fieldName, err := fieldCatalog.Str(fieldNameField)
			if err != nil {
				return nil, fmt.Errorf("field catalog get string error: %w", err)
			}
			fieldType, err := fieldCatalog.Int32(typeField)
			if err != nil {
				return nil, fmt.Errorf("field catalog get int32 error: %w", err)
			}
			fieldLength, err := fieldCatalog.Int32(lengthField)
			if err != nil {
				return nil, fmt.Errorf("field catalog get int32 error: %w", err)
			}
			fieldOffset, err := fieldCatalog.Int32(offsetField)
			if err != nil {
				return nil, fmt.Errorf("field catalog get int32 error: %w", err)
			}
			offsets[schema.FieldName(fieldName)] = fieldOffset
			sche.AddField(schema.FieldName(fieldName), schema.NewField(schema.FieldType(fieldType), fieldLength))
		}
	}
	if err := fieldCatalog.Close(); err != nil {
		return nil, fmt.Errorf("field catalog close error: %w", err)
	}
	if size == -1 {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
	return record.NewLayout(sche, offsets, size), nil
}

type StatInfo struct {
	numBlocks  int
	numRecords int
}

func NewStatInfo(numBlocks, numRecords int) StatInfo {
	return StatInfo{numBlocks: numBlocks, numRecords: numRecords}
}

func (s StatInfo) BlocksAccessed() int {
	return s.numBlocks
}

func (s StatInfo) RecordsOutput() int {
	return s.numRecords
}

func (s StatInfo) DistinctValues(f schema.FieldName) int {
	return 1 + s.numRecords/3
}

type StatManager struct {
	tableManager *TableManager
	tableStats   map[string]StatInfo
	tableStatsMu sync.RWMutex
	numCalls     atomic.Int64
}

func NewStatManager(tableManager *TableManager, tx *transaction.Transaction) (*StatManager, error) {
	m := &StatManager{tableManager: tableManager}
	if err := m.refresh(tx); err != nil {
		return nil, fmt.Errorf("refresh error: %w", err)
	}
	return m, nil
}

func (m *StatManager) refresh(tx *transaction.Transaction) error {
	m.tableStatsMu.Lock()
	defer m.tableStatsMu.Unlock()
	m.numCalls.Store(0)
	m.tableStats = make(map[string]StatInfo)
	tableCatalog, err := record.NewTableScan(tx, tableCatalogTableName, m.tableManager.tableCatalogLayout)
	if err != nil {
		return fmt.Errorf("table catalog scan error: %w", err)
	}
	for {
		if ok, err := tableCatalog.Next(); err != nil {
			return fmt.Errorf("table catalog next error: %w", err)
		} else if !ok {
			break
		}
		name, err := tableCatalog.Str(tableNameField)
		if err != nil {
			return fmt.Errorf("table catalog get string error: %w", err)
		}
		layout, err := m.tableManager.Layout(name, tx)
		if err != nil {
			return fmt.Errorf("layout error: %w", err)
		}
		stat, err := m.calcTableStats(name, layout, tx)
		if err != nil {
			return fmt.Errorf("calc stats error: %w", err)
		}
		m.tableStats[name] = stat
	}
	if err := tableCatalog.Close(); err != nil {
		return fmt.Errorf("table catalog close error: %w", err)
	}
	return nil
}

func (m *StatManager) calcTableStats(tableName string, layout *record.Layout, tx *transaction.Transaction) (StatInfo, error) {
	numRecs := 0
	numBlocks := 0
	scan, err := record.NewTableScan(tx, tableName, layout)
	if err != nil {
		return StatInfo{}, fmt.Errorf("table scan error: %w", err)
	}
	for {
		ok, err := scan.Next()
		if err != nil {
			return StatInfo{}, fmt.Errorf("scan next error: %w", err)
		}
		if !ok {
			break
		}
		numRecs++
		numBlocks = int(scan.RID().BlockNum() + 1)
	}
	if err := scan.Close(); err != nil {
		return StatInfo{}, fmt.Errorf("scan close error: %w", err)
	}
	return NewStatInfo(numBlocks, numRecs), nil
}

func (m *StatManager) StatInfo(tableName string, layout *record.Layout, tx *transaction.Transaction) (StatInfo, error) {
	m.numCalls.Add(1)
	if m.numCalls.Load() > 100 {
		if err := m.refresh(tx); err != nil {
			return StatInfo{}, fmt.Errorf("refresh error: %w", err)
		}
	}
	m.tableStatsMu.RLock()
	statInfo, ok := m.tableStats[tableName]
	m.tableStatsMu.RUnlock()
	if !ok {
		si, err := m.calcTableStats(tableName, layout, tx)
		if err != nil {
			return StatInfo{}, fmt.Errorf("calc stats error: %w", err)
		}
		m.tableStatsMu.Lock()
		m.tableStats[tableName] = si
		m.tableStatsMu.Unlock()
		statInfo = si
	}
	return statInfo, nil
}

const maxViewDef = 100

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *transaction.Transaction) (*ViewManager, error) {
	if isNew {
		s := schema.NewSchema()
		s.AddStrField(viewNameField, maxTableNameLength)
		s.AddStrField(viewDefField, maxViewDef)
		if err := tableManager.CreateTable(viewCatalogTableName, s, tx); err != nil {
			return nil, fmt.Errorf("create view catalog error: %w", err)
		}
	}
	return &ViewManager{tableManager: tableManager}, nil
}

func (m *ViewManager) CreateView(viewName, viewDef string, tx *transaction.Transaction) error {
	layout, err := m.tableManager.Layout(viewCatalogTableName, tx)
	if err != nil {
		return fmt.Errorf("layout error: %w", err)
	}
	scan, err := record.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return fmt.Errorf("table scan error: %w", err)
	}
	if err := scan.Insert(); err != nil {
		return fmt.Errorf("insert error: %w", err)
	}
	if err := scan.SetStr(viewNameField, viewName); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.SetStr(viewDefField, viewDef); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.Close(); err != nil {
		return fmt.Errorf("close error: %w", err)
	}
	return nil
}

func (m *ViewManager) ViewDef(viewName string, tx *transaction.Transaction) (string, bool, error) {
	var (
		res   string
		found bool
	)
	layout, err := m.tableManager.Layout(viewCatalogTableName, tx)
	if err != nil {
		return "", false, fmt.Errorf("layout error: %w", err)
	}
	scan, err := record.NewTableScan(tx, viewCatalogTableName, layout)
	if err != nil {
		return "", false, fmt.Errorf("table scan error: %w", err)
	}
	for {
		if ok, err := scan.Next(); err != nil {
			return "", false, fmt.Errorf("next error: %w", err)
		} else if !ok {
			break
		}
		name, err := scan.Str(viewNameField)
		if err != nil {
			return "", false, fmt.Errorf("get string error: %w", err)
		}
		if name == viewName {
			res, err = scan.Str(viewDefField)
			if err != nil {
				return "", false, fmt.Errorf("get string error: %w", err)
			}
			found = true
			break
		}
	}
	if err := scan.Close(); err != nil {
		return "", false, fmt.Errorf("close error: %w", err)
	}
	return res, found, nil
}

type IndexInfo struct {
	indexName   string
	fieldName   schema.FieldName
	tx          *transaction.Transaction
	tableSchema *schema.Schema
	indexLayout *record.Layout
	statInfo    StatInfo
	cfg         *index.Config
}

func NewIndexInfo(
	indexName string,
	fieldName schema.FieldName,
	tableSchema *schema.Schema,
	tx *transaction.Transaction,
	statInfo StatInfo,
	cfg *index.Config,
) (*IndexInfo, error) {
	ii := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		tx:          tx,
		tableSchema: tableSchema,
		statInfo:    statInfo,
		cfg:         cfg,
	}
	ii.indexLayout = index.NewIndexLayout(schema.NewField(tableSchema.Typ(fieldName), tableSchema.Length(fieldName)))
	return ii, nil
}

func (i *IndexInfo) IndexName() string {
	return i.indexName
}

func (i *IndexInfo) FieldName() schema.FieldName {
	return i.fieldName
}

func (i *IndexInfo) Open() (index.Index, error) {
	idx, err := i.cfg.Initializer(i.tx, i.indexName, i.indexLayout)
	if err != nil {
		return nil, fmt.Errorf("initializer error: %w", err)
	}
	return idx, nil
}

func (i *IndexInfo) BlockAccessed() int {
	rpb := i.tx.BlockSize() / i.indexLayout.SlotSize()
	numBlocks := i.statInfo.RecordsOutput() / int(rpb)
	return i.cfg.SearchCost(numBlocks, int(rpb))
}

func (i *IndexInfo) RecordsOutput() int {
	return i.statInfo.RecordsOutput() / i.statInfo.DistinctValues(i.fieldName)
}

func (i *IndexInfo) DistinctValues(fieldName schema.FieldName) int {
	if fieldName == i.fieldName {
		return 1
	}
	return i.statInfo.DistinctValues(fieldName)
}

type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	statManager  *StatManager
	cfg          *index.Config
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, tx *transaction.Transaction, cfg *Config) (*IndexManager, error) {
	if isNew {
		s := schema.NewSchema()
		s.AddStrField(indexNameField, maxTableNameLength)
		s.AddStrField(tableNameField, maxTableNameLength)
		s.AddStrField(fieldNameField, maxTableNameLength)
		if err := tableManager.CreateTable(indexCatalogTableName, s, tx); err != nil {
			return nil, fmt.Errorf("create index catalog error: %w", err)
		}
	}
	layout, err := tableManager.Layout(indexCatalogTableName, tx)
	if err != nil {
		return nil, fmt.Errorf("layout error: %w", err)
	}
	indexCfg := index.ConfigBTree
	if cfg.Index != nil {
		indexCfg = cfg.Index
	}
	return &IndexManager{layout: layout, tableManager: tableManager, statManager: statManager, cfg: indexCfg}, nil
}

func (m *IndexManager) CreateIndex(indexName, tableName string, fieldName schema.FieldName, tx *transaction.Transaction) error {
	scan, err := record.NewTableScan(tx, indexCatalogTableName, m.layout)
	if err != nil {
		return fmt.Errorf("table scan error: %w", err)
	}
	if err := scan.Insert(); err != nil {
		return fmt.Errorf("insert error: %w", err)
	}
	if err := scan.SetStr(indexNameField, indexName); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.SetStr(tableNameField, tableName); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.SetStr(fieldNameField, string(fieldName)); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.Close(); err != nil {
		return fmt.Errorf("close error: %w", err)
	}
	return nil
}

func (m *IndexManager) IndexInfo(tableName string, tx *transaction.Transaction) (map[schema.FieldName]IndexInfo, error) {
	res := make(map[schema.FieldName]IndexInfo)
	scan, err := record.NewTableScan(tx, indexCatalogTableName, m.layout)
	if err != nil {
		return nil, fmt.Errorf("table scan error: %w", err)
	}
	for {
		if ok, err := scan.Next(); err != nil {
			return nil, fmt.Errorf("next error: %w", err)
		} else if !ok {
			break
		}
		indexName, err := scan.Str(indexNameField)
		if err != nil {
			return nil, fmt.Errorf("get string error: %w", err)
		}
		fieldName, err := scan.Str(fieldNameField)
		if err != nil {
			return nil, fmt.Errorf("get string error: %w", err)
		}
		tableLayout, err := m.tableManager.Layout(tableName, tx)
		if err != nil {
			return nil, fmt.Errorf("layout error: %w", err)
		}
		statInfo, err := m.statManager.StatInfo(tableName, tableLayout, tx)
		if err != nil {
			return nil, fmt.Errorf("stat error: %w", err)
		}
		initializer := m.cfg.Initializer
		if initializer == nil {
			initializer = index.NewHashIndex
		}
		indexInfo, err := NewIndexInfo(indexName, schema.FieldName(fieldName), tableLayout.Schema(), tx, statInfo, m.cfg)
		if err != nil {
			return nil, fmt.Errorf("new index info error: %w", err)
		}
		res[schema.FieldName(fieldName)] = *indexInfo
	}
	if err := scan.Close(); err != nil {
		return nil, fmt.Errorf("close error: %w", err)
	}
	return res, nil
}

type Manager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	indexManager *IndexManager
	statManager  *StatManager
}

func NewManager(isNew bool, tx *transaction.Transaction, cfg *Config) (*Manager, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	tableManager, err := NewTableManager(isNew, tx)
	if err != nil {
		return nil, fmt.Errorf("new table manager error: %w", err)
	}
	viewManager, err := NewViewManager(isNew, tableManager, tx)
	if err != nil {
		return nil, fmt.Errorf("new view manager error: %w", err)
	}
	statManager, err := NewStatManager(tableManager, tx)
	if err != nil {
		return nil, fmt.Errorf("new stat manager error: %w", err)
	}
	indexManager, err := NewIndexManager(isNew, tableManager, statManager, tx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new index manager error: %w", err)
	}
	return &Manager{
		tableManager: tableManager,
		viewManager:  viewManager,
		indexManager: indexManager,
		statManager:  statManager,
	}, nil
}

func (m *Manager) CreateTable(tableName string, s schema.Schema, tx *transaction.Transaction) error {
	if err := m.tableManager.CreateTable(tableName, s, tx); err != nil {
		return fmt.Errorf("create table error: %w", err)
	}
	return nil
}

func (m *Manager) Layout(tableName string, tx *transaction.Transaction) (*record.Layout, error) {
	layout, err := m.tableManager.Layout(tableName, tx)
	if err != nil {
		return nil, fmt.Errorf("layout error: %w", err)
	}
	return layout, nil
}

func (m *Manager) CreateView(viewName, viewDef string, tx *transaction.Transaction) error {
	if err := m.viewManager.CreateView(viewName, viewDef, tx); err != nil {
		return fmt.Errorf("create view error: %w", err)
	}
	return nil
}

func (m *Manager) ViewDef(viewName string, tx *transaction.Transaction) (string, bool, error) {
	viewDef, ok, err := m.viewManager.ViewDef(viewName, tx)
	if err != nil {
		return "", false, fmt.Errorf("view def error: %w", err)
	}
	return viewDef, ok, nil
}

func (m *Manager) CreateIndex(indexName, tableName string, fieldName schema.FieldName, tx *transaction.Transaction) error {
	if err := m.indexManager.CreateIndex(indexName, tableName, fieldName, tx); err != nil {
		return fmt.Errorf("create index error: %w", err)
	}
	return nil
}

func (m *Manager) IndexInfo(tableName string, tx *transaction.Transaction) (map[schema.FieldName]IndexInfo, error) {
	res, err := m.indexManager.IndexInfo(tableName, tx)
	if err != nil {
		return nil, fmt.Errorf("index info error: %w", err)
	}
	return res, nil
}

func (m *Manager) StatInfo(tableName string, layout *record.Layout, tx *transaction.Transaction) (StatInfo, error) {
	statInfo, err := m.statManager.StatInfo(tableName, layout, tx)
	if err != nil {
		return StatInfo{}, fmt.Errorf("stat error: %w", err)
	}
	return statInfo, nil
}

type Config struct {
	Index *index.Config
}
