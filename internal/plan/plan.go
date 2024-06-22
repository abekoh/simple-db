package plan

import (
	"github.com/abekoh/simple-db/internal/parse"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type Plan interface {
	Open() (query.Scan, error)
	BlockAccessed() (int, error)
	RecordsOutput() (int, error)
	DistinctValues(fieldName string) (int, error)
	Schema() *schema.Schema
}

type QueryPlanner interface {
	CreatePlan(d *parse.QueryData, tx *transaction.Transaction) (Plan, error)
}

type UpdatePlanner interface {
	ExecuteInsert(d *parse.InsertData, tx *transaction.Transaction) (int, error)
	ExecuteDelete(d *parse.DeleteData, tx *transaction.Transaction) (int, error)
	ExecuteModify(d *parse.ModifyData, tx *transaction.Transaction) (int, error)
	ExecuteCreateTable(d *parse.CreateTableData, tx *transaction.Transaction) (int, error)
	ExecuteCreateView(d *parse.CreateViewData, tx *transaction.Transaction) (int, error)
	ExecuteCreateIndex(d *parse.CreateIndexData, tx *transaction.Transaction) (int, error)
}
