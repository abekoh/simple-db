package index

import "github.com/abekoh/simple-db/internal/record"

type Index interface {
	BeforeFirst(searchKey string) error
	Next() (bool, error)
	DataRID() (record.RID, error)
	Insert(searchKey string, dataRID record.RID) error
	Delete(searchKey string, dataRID record.RID) error
	Close() error
}
