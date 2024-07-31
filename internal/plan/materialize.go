package plan

import (
	"fmt"
	"math"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
	"github.com/oklog/ulid/v2"
)

type TempTable struct {
	tx        *transaction.Transaction
	tableName string
	layout    *record.Layout
}

func NewTempTable(tx *transaction.Transaction, sche schema.Schema) *TempTable {
	l := record.NewLayoutSchema(sche)
	return &TempTable{tx: tx, tableName: fmt.Sprintf("temp_table_%s", ulid.Make()), layout: l}
}

func (t TempTable) Open() (query.UpdateScan, error) {
	ts, err := record.NewTableScan(t.tx, t.tableName, t.layout)
	if err != nil {
		return nil, fmt.Errorf("record.NewTableScan error: %w", err)
	}
	return ts, nil
}

func (t TempTable) TableName() string {
	return t.tableName
}

func (t TempTable) Layout() *record.Layout {
	return t.layout
}

type MaterializePlan struct {
	srcPlan Plan
	tx      *transaction.Transaction
}

var _ Plan = (*MaterializePlan)(nil)

func NewMaterializePlan(tx *transaction.Transaction, p Plan) *MaterializePlan {
	return &MaterializePlan{srcPlan: p, tx: tx}
}

func (p MaterializePlan) Result() {}

func (p MaterializePlan) String() string {
	return fmt.Sprintf("Materialize{%s}", p.srcPlan)
}

func (p MaterializePlan) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	return p.srcPlan.Placeholders(findSchema)
}

func (p MaterializePlan) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	newSrcPlan, err := p.srcPlan.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("srcPlan.SwapParams error: %w", err)
	}
	np, ok := newSrcPlan.(BoundPlan)
	if !ok {
		return nil, fmt.Errorf("newSrcPlan is not a plan.Plan")
	}
	return &BoundPlan{
		Plan: NewMaterializePlan(p.tx, np.Plan),
	}, nil
}

func (p MaterializePlan) Open() (query.Scan, error) {
	sche := p.srcPlan.Schema()
	temp := NewTempTable(p.tx, *sche)
	src, err := p.srcPlan.Open()
	if err != nil {
		return nil, fmt.Errorf("temp.Open error: %w", err)
	}
	dest, err := temp.Open()
	if err != nil {
		return nil, fmt.Errorf("temp.Open error: %w", err)
	}
	for {
		ok, err := src.Next()
		if err != nil {
			return nil, fmt.Errorf("src.Next error: %w", err)
		}
		if !ok {
			break
		}
		if err := dest.Insert(); err != nil {
			return nil, fmt.Errorf("us.Insert error: %w", err)
		}
		for _, fldName := range sche.FieldNames() {
			val, err := src.Val(fldName)
			if err != nil {
				return nil, fmt.Errorf("src.Val error: %w", err)
			}
			if err := dest.SetVal(fldName, val); err != nil {
				return nil, fmt.Errorf("us.SetVal error: %w", err)
			}
		}
	}
	if err := src.Close(); err != nil {
		return nil, fmt.Errorf("src.Close error: %w", err)
	}
	if err := dest.BeforeFirst(); err != nil {
		return nil, fmt.Errorf("us.BeforeFirst error: %w", err)
	}
	return dest, nil
}

func (p MaterializePlan) BlockAccessed() int {
	l := record.NewLayoutSchema(*p.srcPlan.Schema())
	rpb := float64(p.tx.BlockSize()) / float64(l.SlotSize())
	return int(math.Ceil(float64(p.srcPlan.RecordsOutput()) / rpb))
}

func (p MaterializePlan) RecordsOutput() int {
	return p.srcPlan.RecordsOutput()
}

func (p MaterializePlan) DistinctValues(fieldName schema.FieldName) int {
	return p.srcPlan.DistinctValues(fieldName)
}

func (p MaterializePlan) Schema() *schema.Schema {
	return p.srcPlan.Schema()
}
