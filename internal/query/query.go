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
	Val(fieldName string) (Constant, error)
	HasField(fieldName string) bool
	Close() error
}

type UpdateScan interface {
	Scan
	SetVal(fieldName string, val Constant) error
	SetInt32(fieldName string, val int32) error
	SetStr(fieldName string, val string) error
	Insert() error
	Delete() error
	RID() record.RID
	MoveToRID(rid record.RID) error
}

type Constant interface {
	fmt.Stringer
	Val() any
}

type ConstantInt32 int32

func (v ConstantInt32) String() string {
	return fmt.Sprintf("%d", v)
}

func (v ConstantInt32) Val() any {
	return int32(v)
}

type ConstantStr string

func (v ConstantStr) String() string {
	return string(v)
}

func (v ConstantStr) Val() any {
	return string(v)
}
