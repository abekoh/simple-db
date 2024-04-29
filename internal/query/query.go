package query

import (
	"fmt"
	"strings"

	"github.com/abekoh/simple-db/internal/record/schema"
)

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
	RID() schema.RID
	MoveToRID(rid schema.RID) error
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

func (t Term) String() string {
	return fmt.Sprintf("%v=%v", t.lhs, t.rhs)
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

func (p Predicate) IsSatisfied(scan Scan) (bool, error) {
	for _, term := range p {
		ok, err := term.IsSatisfied(scan)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (p Predicate) String() string {
	var sb strings.Builder
	for i, term := range p {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString(term.String())
	}
	return sb.String()
}

var _ UpdateScan = (*SelectScan)(nil)

type SelectScan struct {
	UpdateScan
	pred Predicate
}

func NewSelectScan(updateScan UpdateScan, pred Predicate) *SelectScan {
	return &SelectScan{UpdateScan: updateScan, pred: pred}
}

func (s SelectScan) Next() (bool, error) {
	for {
		ok, err := s.UpdateScan.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		ok, err = s.pred.IsSatisfied(s.UpdateScan)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
}
