package index

import (
	"github.com/abekoh/simple-db/internal/query"
)

type Index interface {
	BeforeFirst(searchKey string) error
	Next() (bool, error)
	DataRID() (query.RID, error)
	Insert(searchKey string, dataRID query.RID) error
	Delete(searchKey string, dataRID query.RID) error
	Close() error
}
