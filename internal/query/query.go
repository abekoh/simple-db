package query

import (
	"fmt"
	"math"
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
	AppliesTo(s *schema.Schema) bool
}

type Term struct {
	lhs, rhs Expression
}

func NewTerm(lhs, rhs Expression) Term {
	return Term{lhs: lhs, rhs: rhs}
}

func (t Term) Expressions() (lhs, rhs Expression) {
	return t.lhs, t.rhs
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

func (t Term) ReductionFactor(distinctValues func(fn schema.FieldName) int) int {
	lhsFieldName, isLHSFieldName := t.lhs.(schema.FieldName)
	rhsFieldName, isRHSFieldName := t.rhs.(schema.FieldName)
	if isLHSFieldName && isRHSFieldName {
		return max(distinctValues(lhsFieldName), distinctValues(rhsFieldName))
	} else if isLHSFieldName {
		return distinctValues(lhsFieldName)
	} else if isRHSFieldName {
		return distinctValues(rhsFieldName)
	}
	lhsConst, isLHSConst := t.lhs.(schema.Constant)
	rhsConst, isRHSConst := t.rhs.(schema.Constant)
	if isLHSConst && isRHSConst && lhsConst == rhsConst {
		return 1
	}
	return math.MaxInt
}

func (t Term) EquatesWithConstant(fieldName schema.FieldName) (schema.Constant, bool) {
	lhsFieldName, isLHSFieldName := t.lhs.(schema.FieldName)
	rhsFieldName, isRHSFieldName := t.rhs.(schema.FieldName)
	if isLHSFieldName && !isRHSFieldName && lhsFieldName == fieldName {
		return t.rhs.(schema.Constant), true
	}
	if isRHSFieldName && !isLHSFieldName && rhsFieldName == fieldName {
		return t.lhs.(schema.Constant), true
	}
	return nil, false
}

func (t Term) EquatesWithField(fieldName schema.FieldName) (schema.FieldName, bool) {
	lhsFieldName, isLHSFieldName := t.lhs.(schema.FieldName)
	rhsFieldName, isRHSFieldName := t.rhs.(schema.FieldName)
	if isLHSFieldName && isRHSFieldName && lhsFieldName == fieldName {
		return rhsFieldName, true
	}
	if isRHSFieldName && isLHSFieldName && rhsFieldName == fieldName {
		return lhsFieldName, true
	}
	return "", false
}

func (t Term) AppliesTo(s *schema.Schema) bool {
	return t.lhs.AppliesTo(s) && t.rhs.AppliesTo(s)
}

type Predicate []Term

func NewPredicate(terms ...Term) Predicate {
	return terms
}

func (p Predicate) SwapParams(params map[int]schema.Constant) (Predicate, error) {
	swapExpression := func(expr Expression) (Expression, error) {
		if placeholder, ok := expr.(schema.Placeholder); ok {
			val, ok := params[int(placeholder)]
			if !ok {
				return nil, fmt.Errorf("missing parameter: %d", placeholder)
			}
			e, err := val.(Expression)
			if !err {
				return nil, fmt.Errorf("parameter is not an expression: %v", val)
			}
			return e, nil
		}
		return expr, nil
	}
	swapped := make(Predicate, len(p))
	for i, term := range p {
		lhs, err := swapExpression(term.lhs)
		if err != nil {
			return nil, fmt.Errorf("lhs swap error: %w", err)
		}
		rhs, err := swapExpression(term.rhs)
		if err != nil {
			return nil, fmt.Errorf("rhs swap error: %w", err)
		}
		swapped[i] = NewTerm(lhs, rhs)
	}
	return swapped, nil
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

func (p Predicate) EquatesWithConstant(fieldName schema.FieldName) (schema.Constant, bool) {
	for _, term := range p {
		if c, ok := term.EquatesWithConstant(fieldName); ok {
			return c, true
		}
	}
	return nil, false
}

func (p Predicate) EquatesWithField(fieldName schema.FieldName) (schema.FieldName, bool) {
	for _, term := range p {
		if fn, ok := term.EquatesWithField(fieldName); ok {
			return fn, true
		}
	}
	return "", false
}

func (p Predicate) SelectSubPred(sche *schema.Schema) (Predicate, bool) {
	result := make(Predicate, 0, len(p))
	for _, t := range p {
		if t.AppliesTo(sche) {
			result = append(result, t)
		}
	}
	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

func (p Predicate) JoinSubPred(sche1, sche2 *schema.Schema) (Predicate, bool) {
	result := make(Predicate, 0, len(p))
	newSche := schema.NewSchema()
	newSche.AddAll(*sche1)
	newSche.AddAll(*sche2)
	for _, t := range p {
		if !t.AppliesTo(sche1) && !t.AppliesTo(sche2) && t.AppliesTo(&newSche) {
			result = append(result, t)
		}
	}
	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

var _ UpdateScan = (*SelectScan)(nil)

type SelectScan struct {
	Scan
	pred Predicate
}

func NewSelectScan(scan Scan, pred Predicate) *SelectScan {
	return &SelectScan{Scan: scan, pred: pred}
}

func (s *SelectScan) Next() (bool, error) {
	for {
		ok, err := s.Scan.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		ok, err = s.pred.IsSatisfied(s.Scan)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
}

func (s *SelectScan) SetVal(fieldName schema.FieldName, val schema.Constant) error {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return fmt.Errorf("scan is not updatable")
	}
	return us.SetVal(fieldName, val)
}

func (s *SelectScan) SetInt32(fieldName schema.FieldName, val int32) error {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return fmt.Errorf("scan is not updatable")
	}
	return us.SetInt32(fieldName, val)
}

func (s *SelectScan) SetStr(fieldName schema.FieldName, val string) error {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return fmt.Errorf("scan is not updatable")
	}
	return us.SetStr(fieldName, val)
}

func (s *SelectScan) Insert() error {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return fmt.Errorf("scan is not updatable")
	}
	return us.Insert()
}

func (s *SelectScan) Delete() error {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return fmt.Errorf("scan is not updatable")
	}
	return us.Delete()
}

func (s *SelectScan) RID() schema.RID {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return schema.RID{}
	}
	return us.RID()
}

func (s *SelectScan) MoveToRID(rid schema.RID) error {
	us, ok := s.Scan.(UpdateScan)
	if !ok {
		return fmt.Errorf("scan is not updatable")
	}
	return us.MoveToRID(rid)
}

type ProductScan struct {
	scan1, scan2 Scan
}

var _ Scan = (*ProductScan)(nil)

func NewProductScan(scan1, scan2 Scan) (*ProductScan, error) {
	s := &ProductScan{scan1: scan1, scan2: scan2}
	if err := s.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("BeforeFirst error: %w", err)
	}
	return s, nil
}

func (s *ProductScan) BeforeFirst() error {
	if err := s.scan1.BeforeFirst(); err != nil {
		return fmt.Errorf("scan1.BeforeFirst error: %w", err)
	}
	if _, err := s.scan1.Next(); err != nil {
		return fmt.Errorf("scan1.Next error: %w", err)
	}
	if err := s.scan2.BeforeFirst(); err != nil {
		return fmt.Errorf("scan2.BeforeFirst error: %w", err)
	}
	return nil
}

func (s *ProductScan) Next() (bool, error) {
	ok, err := s.scan2.Next()
	if err != nil {
		return false, fmt.Errorf("scan2.Next error: %w", err)
	}
	if ok {
		return true, nil
	}
	if err := s.scan2.BeforeFirst(); err != nil {
		return false, fmt.Errorf("scan2.BeforeFirst error: %w", err)
	}
	scan2Ok, err := s.scan2.Next()
	if err != nil {
		return false, fmt.Errorf("scan2.Next error: %w", err)
	}
	scan1Ok, err := s.scan1.Next()
	if err != nil {
		return false, fmt.Errorf("scan1.Next error: %w", err)
	}
	return scan2Ok && scan1Ok, nil
}

func (s *ProductScan) Int32(fieldName schema.FieldName) (int32, error) {
	if s.scan1.HasField(fieldName) {
		return s.scan1.Int32(fieldName)
	}
	return s.scan2.Int32(fieldName)
}

func (s *ProductScan) Str(fieldName schema.FieldName) (string, error) {
	if s.scan1.HasField(fieldName) {
		return s.scan1.Str(fieldName)
	}
	return s.scan2.Str(fieldName)
}

func (s *ProductScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	if s.scan1.HasField(fieldName) {
		return s.scan1.Val(fieldName)
	}
	return s.scan2.Val(fieldName)
}

func (s *ProductScan) HasField(fieldName schema.FieldName) bool {
	return s.scan1.HasField(fieldName) || s.scan2.HasField(fieldName)
}

func (s *ProductScan) Close() error {
	if err := s.scan1.Close(); err != nil {
		return fmt.Errorf("scan1.Close error: %w", err)
	}
	if err := s.scan2.Close(); err != nil {
		return fmt.Errorf("scan2.Close error: %w", err)
	}
	return nil
}

type ProjectScan struct {
	Scan
	fieldNameSet map[schema.FieldName]struct{}
}

var _ Scan = (*ProjectScan)(nil)

func NewProjectScan(scan Scan, fieldNames ...schema.FieldName) *ProjectScan {
	fieldNameSet := make(map[schema.FieldName]struct{}, len(fieldNames))
	for _, fieldName := range fieldNames {
		fieldNameSet[fieldName] = struct{}{}
	}
	return &ProjectScan{Scan: scan, fieldNameSet: fieldNameSet}
}

func (s *ProjectScan) Int32(fieldName schema.FieldName) (int32, error) {
	if !s.HasField(fieldName) {
		return 0, fmt.Errorf("field %s not found", fieldName)
	}
	return s.Scan.Int32(fieldName)
}

func (s *ProjectScan) Str(fieldName schema.FieldName) (string, error) {
	if !s.HasField(fieldName) {
		return "", fmt.Errorf("field %s not found", fieldName)
	}
	return s.Scan.Str(fieldName)
}

func (s *ProjectScan) Val(fieldName schema.FieldName) (schema.Constant, error) {
	if !s.HasField(fieldName) {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}
	return s.Scan.Val(fieldName)
}

func (s *ProjectScan) HasField(fieldName schema.FieldName) bool {
	_, ok := s.fieldNameSet[fieldName]
	return ok
}

type OrderType int

const (
	Asc OrderType = iota
	Desc
)

type (
	Order        []OrderElement
	OrderElement struct {
		field     schema.FieldName
		orderType OrderType
	}
)

func NewOrder(elements ...OrderElement) Order {
	return elements
}

func (o Order) Compare(s1, s2 Scan) (int, error) {
	for _, el := range o {
		val1, err := s1.Val(el.field)
		if err != nil {
			return 0, fmt.Errorf("s1.Val error: %w", err)
		}
		val2, err := s2.Val(el.field)
		if err != nil {
			return 0, fmt.Errorf("s2.Val error: %w", err)
		}
		cmp := val1.Compare(val2)
		if cmp != 0 {
			if el.orderType == Desc {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}
