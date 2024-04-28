package query

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/record/schema"
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
	schema.Valuable
	BeforeFirst() error
	Next() (bool, error)
	Int32(fieldName schema.FieldName) (int32, error)
	Str(fieldName schema.FieldName) (string, error)
	HasField(fieldName schema.FieldName) bool
	Close() error
}

type UpdateScan interface {
	Scan
	SetVal(fieldName schema.FieldName, val schema.Constant) error
	SetInt32(fieldName schema.FieldName, val int32) error
	SetStr(fieldName schema.FieldName, val string) error
	Insert() error
	Delete() error
	RID() RID
	MoveToRID(rid RID) error
}

type Expression interface {
	Evaluate(v schema.Valuable) (schema.Constant, error)
}

type Term struct {
	lhs, rhs Expression
}

func NewTerm(lhs, rhs Expression) Term {
	return Term{lhs: lhs, rhs: rhs}
}

func (t Term) IsSatisfied(scan Scan) (bool, error) {
	lhsVal, err := t.lhs.Evaluate(scan)
	if err != nil {
		return false, fmt.Errorf("lhs evaluation error: %w", err)
	}
	rhsVsl, err := t.rhs.Evaluate(scan)
	if err != nil {
		return false, fmt.Errorf("rhs evaluation error: %w", err)
	}
	switch lhsVal.(type) {
	case schema.ConstantInt32:
		if _, ok := rhsVsl.(schema.ConstantInt32); !ok {
			return false, fmt.Errorf("rhs is not int32")
		}
		return lhsVal.Val().(int32) == rhsVsl.Val().(int32), nil
	case schema.ConstantStr:
		if _, ok := rhsVsl.(schema.ConstantStr); !ok {
			return false, fmt.Errorf("rhs is not string")
		}
		return lhsVal.Val().(string) == rhsVsl.Val().(string), nil
	default:
		return false, fmt.Errorf("lhs, rhs type mismatch")
	}
}

type Predicate []Term

func NewPredicate(terms ...Term) Predicate {
	return terms
}
