package plan

import (
	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Plan interface {
	Open() (query.Scan, error)
	BlockAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName schema.FieldName) int
	Schema() *schema.Schema
}

type TablePlan struct {
	tableName string
	tx        *transaction.Transaction
	layout    *record.Layout
	statInfo  *metadata.StatInfo
}

var _ Plan = (*TablePlan)(nil)

func NewTablePlan(
	tableName string,
	tx *transaction.Transaction,
	layout *record.Layout,
	statInfo *metadata.StatInfo) *TablePlan {
	return &TablePlan{
		tableName: tableName,
		tx:        tx,
		layout:    layout,
		statInfo:  statInfo,
	}
}

func (t TablePlan) Open() (query.Scan, error) {
	return record.NewTableScan(t.tx, t.tableName, t.layout)
}

func (t TablePlan) BlockAccessed() int {
	return t.statInfo.BlocksAccessed()
}

func (t TablePlan) RecordsOutput() int {
	return t.statInfo.RecordsOutput()
}

func (t TablePlan) DistinctValues(fieldName schema.FieldName) int {
	return t.statInfo.DistinctValues(fieldName)
}

func (t TablePlan) Schema() *schema.Schema {
	return t.layout.Schema()
}
