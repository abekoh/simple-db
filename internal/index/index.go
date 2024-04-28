package index

import (
	"github.com/abekoh/simple-db/internal/record/schema"
)

type Index interface {
	BeforeFirst(searchKey string) error
	Next() (bool, error)
	DataRID() (schema.RID, error)
	Insert(searchKey string, dataRID schema.RID) error
	Delete(searchKey string, dataRID schema.RID) error
	Close() error
}
