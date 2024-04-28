package query

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/record"
)

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	Int32(fieldName string) (int32, error)
	Str(fieldName string) (string, error)
	Val(fieldName string) (Value, error)
	HasField(fieldName string) bool
	Close() error
}

type UpdateScan interface {
	Scan
	SetVal(fieldName string, val Value) error
	SetInt32(fieldName string, val int32) error
	SetStr(fieldName string, val string) error
	Insert() error
	Delete() error
	RID() record.RID
	MoveToRID(rid record.RID) error
}

type Value interface {
	fmt.Stringer
	Val() any
}

type ValueInt32 int32

func (v ValueInt32) String() string {
	return fmt.Sprintf("%d", v)
}

func (v ValueInt32) Val() any {
	return int32(v)
}

type ValueStr string

func (v ValueStr) String() string {
	return fmt.Sprintf("%s", v)
}

func (v ValueStr) Val() any {
	return string(v)
}
