package metadata

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/transaction"
)

const maxTableNameLength = 16

type TableManager struct {
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

func NewTableManager(isNew bool, tx *transaction.Transaction) (*TableManager, error) {
	m := &TableManager{}

	tableCatalogSchema := record.NewSchema()
	tableCatalogSchema.AddStrField("table_name", maxTableNameLength)
	tableCatalogSchema.AddInt32Field("slot_size")
	m.tableCatalogLayout = record.NewLayoutSchema(tableCatalogSchema)

	fieldCatalogSchema := record.NewSchema()
	fieldCatalogSchema.AddStrField("table_name", maxTableNameLength)
	fieldCatalogSchema.AddStrField("field_name", maxTableNameLength)
	fieldCatalogSchema.AddInt32Field("type")
	fieldCatalogSchema.AddInt32Field("length")
	fieldCatalogSchema.AddInt32Field("offset")
	m.fieldCatalogLayout = record.NewLayoutSchema(fieldCatalogSchema)

	if isNew {
		if err := m.CreateTable("table_catalog", tableCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("create table catalog error: %w", err)
		}
		if err := m.CreateTable("field_catalog", fieldCatalogSchema, tx); err != nil {
			return nil, fmt.Errorf("create field catalog error: %w", err)
		}
	}

	return m, nil
}

func (m *TableManager) CreateTable(tableName string, schema record.Schema, tx *transaction.Transaction) error {
	layout := record.NewLayoutSchema(schema)
	tableCatalog, err := record.NewTableScan(tx, "table_catalog", m.tableCatalogLayout)
	if err != nil {
		return fmt.Errorf("table catalog scan error: %w", err)
	}
	if err := tableCatalog.Insert(); err != nil {
		return fmt.Errorf("table catalog insert error: %w", err)
	}
	if err := tableCatalog.SetStr("table_name", tableName); err != nil {
		return fmt.Errorf("table catalog set string error: %w", err)
	}
	if err := tableCatalog.SetInt32("slot_size", layout.SlotSize()); err != nil {
		return fmt.Errorf("table catalog set int32 error: %w", err)
	}
	if err := tableCatalog.Close(); err != nil {
		return fmt.Errorf("table catalog close error: %w", err)
	}

	fieldCatalog, err := record.NewTableScan(tx, "field_catalog", m.fieldCatalogLayout)
	if err != nil {
		return fmt.Errorf("field catalog scan error: %w", err)
	}
	for _, fieldName := range schema.FieldNames() {
		if err := fieldCatalog.Insert(); err != nil {
			return fmt.Errorf("field catalog insert error: %w", err)
		}
		if err := fieldCatalog.SetStr("table_name", tableName); err != nil {
			return fmt.Errorf("field catalog set string error: %w", err)
		}
		if err := fieldCatalog.SetStr("field_name", fieldName); err != nil {
			return fmt.Errorf("field catalog set string error: %w", err)
		}
		if err := fieldCatalog.SetInt32("type", int32(schema.Typ(fieldName))); err != nil {
			return fmt.Errorf("field catalog set int32 error: %w", err)
		}
		if err := fieldCatalog.SetInt32("length", schema.Length(fieldName)); err != nil {
			return fmt.Errorf("field catalog set int32 error: %w", err)
		}
		offset, ok := layout.Offset(fieldName)
		if !ok {
			return fmt.Errorf("offset not found for %s", fieldName)
		}
		if err := fieldCatalog.SetInt32("offset", offset); err != nil {
			return fmt.Errorf("field catalog set int32 error: %w", err)
		}
	}
	if err := fieldCatalog.Close(); err != nil {
		return fmt.Errorf("field catalog close error: %w", err)
	}
	return nil
}

func (m *TableManager) Layout(tableName string, tx *transaction.Transaction) (*record.Layout, bool, error) {
	var size int32 = -1
	tableCatalog, err := record.NewTableScan(tx, "table_catalog", m.tableCatalogLayout)
	if err != nil {
		return nil, false, fmt.Errorf("table catalog scan error: %w", err)
	}
	for {
		if ok, err := tableCatalog.Next(); err != nil {
			return nil, false, fmt.Errorf("table catalog next error: %w", err)
		} else if !ok {
			break
		}
		if name, err := tableCatalog.Str("table_name"); err != nil {
			return nil, false, fmt.Errorf("table catalog get string error: %w", err)
		} else if name == tableName {
			s, err := tableCatalog.Int32("slot_size")
			if err != nil {
				return nil, false, fmt.Errorf("table catalog get int32 error: %w", err)
			}
			size = s
			break
		}
	}
	if err := tableCatalog.Close(); err != nil {
		return nil, false, fmt.Errorf("table catalog close error: %w", err)
	}

	schema := record.NewSchema()
	offsets := make(map[string]int32)
	fieldCatalog, err := record.NewTableScan(tx, "field_catalog", m.fieldCatalogLayout)
	if err != nil {
		return nil, false, fmt.Errorf("field catalog scan error: %w", err)
	}
	for {
		if ok, err := fieldCatalog.Next(); err != nil {
			return nil, false, fmt.Errorf("field catalog next error: %w", err)
		} else if !ok {
			break
		}
		if name, err := fieldCatalog.Str("table_name"); err != nil {
			return nil, false, fmt.Errorf("field catalog get string error: %w", err)
		} else if name == tableName {
			fieldName, err := fieldCatalog.Str("field_name")
			if err != nil {
				return nil, false, fmt.Errorf("field catalog get string error: %w", err)
			}
			fieldType, err := fieldCatalog.Int32("type")
			if err != nil {
				return nil, false, fmt.Errorf("field catalog get int32 error: %w", err)
			}
			fieldLength, err := fieldCatalog.Int32("length")
			if err != nil {
				return nil, false, fmt.Errorf("field catalog get int32 error: %w", err)
			}
			fieldOffset, err := fieldCatalog.Int32("offset")
			if err != nil {
				return nil, false, fmt.Errorf("field catalog get int32 error: %w", err)
			}
			offsets[fieldName] = fieldOffset
			schema.AddField(fieldName, record.NewField(record.FieldType(fieldType), fieldLength))
		}
	}
	if err := fieldCatalog.Close(); err != nil {
		return nil, false, fmt.Errorf("field catalog close error: %w", err)
	}
	if size == -1 {
		return nil, false, nil
	}
	return record.NewLayout(schema, offsets, size), true, nil
}

const maxViewDef = 1000

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *transaction.Transaction) *ViewManager {
	m := &ViewManager{tableManager: tableManager}
	if isNew {
		schema := record.NewSchema()
		schema.AddStrField("view_name", maxTableNameLength)
		schema.AddStrField("view_def", maxViewDef)
		if err := tableManager.CreateTable("view_catalog", schema, tx); err != nil {
			panic(fmt.Errorf("create view catalog error: %w", err))
		}
	}
	return m
}

func (m *ViewManager) CreateView(viewName, viewDef string, tx *transaction.Transaction) error {
	layout, ok, err := m.tableManager.Layout("view_catalog", tx)
	if err != nil {
		return fmt.Errorf("layout error: %w", err)
	}
	if !ok {
		return fmt.Errorf("view catalog not found")
	}
	scan, err := record.NewTableScan(tx, "view_catalog", layout)
	if err != nil {
		return fmt.Errorf("table scan error: %w", err)
	}
	if err := scan.Insert(); err != nil {
		return fmt.Errorf("insert error: %w", err)
	}
	if err := scan.SetStr("view_name", viewName); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.SetStr("view_def", viewDef); err != nil {
		return fmt.Errorf("set string error: %w", err)
	}
	if err := scan.Close(); err != nil {
		return fmt.Errorf("close error: %w", err)
	}
	return nil
}

func (m *ViewManager) ViewDef(viewName string, tx *transaction.Transaction) (string, bool, error) {
	var res string
	layout, ok, err := m.tableManager.Layout("view_catalog", tx)
	if err != nil {
		return "", false, fmt.Errorf("layout error: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	scan, err := record.NewTableScan(tx, "view_catalog", layout)
	if err != nil {
		return "", false, fmt.Errorf("table scan error: %w", err)
	}
	for {
		if ok, err := scan.Next(); err != nil {
			return "", false, fmt.Errorf("next error: %w", err)
		} else if !ok {
			break
		}
		name, err := scan.Str("view_name")
		if err != nil {
			return "", false, fmt.Errorf("get string error: %w", err)
		}
		if name == viewName {
			res, err = scan.Str("view_def")
			if err != nil {
				return "", false, fmt.Errorf("get string error: %w", err)
			}
			break
		}
	}
	if err := scan.Close(); err != nil {
		return "", false, fmt.Errorf("close error: %w", err)
	}
	return res, true, nil
}
