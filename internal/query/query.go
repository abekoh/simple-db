package query

import (
	"fmt"
)

type RID struct {
	blockNum int32
	slot     int32
}

func NewRID(blockNum, slot int32) RID {
	return RID{blockNum: blockNum, slot: slot}
}

func (r RID) BlockNum() int32 {
	return r.blockNum
}

func (r RID) Slot() int32 {
	return r.slot
}

func (r RID) String() string {
	return fmt.Sprintf("RID{blockNum=%d, slot=%d}", r.blockNum, r.slot)
}

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	Int32(fieldName FieldName) (int32, error)
	Str(fieldName FieldName) (string, error)
	Val(fieldName FieldName) (Constant, error)
	HasField(fieldName FieldName) bool
	Close() error
}

type UpdateScan interface {
	Scan
	SetVal(fieldName FieldName, val Constant) error
	SetInt32(fieldName FieldName, val int32) error
	SetStr(fieldName FieldName, val string) error
	Insert() error
	Delete() error
	RID() RID
	MoveToRID(rid RID) error
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

func (v ConstantInt32) Evaluate(Scan) (Constant, error) {
	return v, nil
}

type ConstantStr string

func (v ConstantStr) String() string {
	return string(v)
}

func (v ConstantStr) Val() any {
	return string(v)
}

func (v ConstantStr) Evaluate(Scan) (Constant, error) {
	return v, nil
}

type Expression interface {
	Evaluate(scan Scan) (Constant, error)
}

type FieldName string

func (f FieldName) Evaluate(scan Scan) (Constant, error) {
	return scan.Val(f)
}
