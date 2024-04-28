package query

import "github.com/abekoh/simple-db/internal/record"

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	Int32(fieldName string) (int32, error)
	Str(fieldName string) (string, error)
	Val(fieldName string) (any, error)
	HasField(fieldName string) bool
	Close() error
}

type UpdateScan interface {
	Scan
	SetVal(fieldName string, val any) error
	SetInt32(fieldName string, val int32) error
	SetStr(fieldName string, val string) error
	Insert() error
	Delete() error
	RID() (record.RID, error)
	MoveToRID(rid record.RID) error
}
