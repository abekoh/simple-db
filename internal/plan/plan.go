package plan

import (
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
)

type Plan interface {
	Open() (query.Scan, error)
	BlockAccessed() (int, error)
	RecordsOutput() (int, error)
	DistinctValues(fieldName string) (int, error)
	Schema() *schema.Schema
}
