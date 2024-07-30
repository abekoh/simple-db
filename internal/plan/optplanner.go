package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/multibuffer"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

type TablePlanner struct {
	myPlan  *TablePlan
	myPred  query.Predicate
	mySche  schema.Schema
	indexes map[schema.FieldName]metadata.IndexInfo
	tx      *transaction.Transaction
}

func NewTablePlanner(
	tableName string,
	myPred query.Predicate,
	tx *transaction.Transaction,
	mdm *metadata.Manager,
) (*TablePlanner, error) {
	myPlan, err := NewTablePlan(tableName, tx, mdm)
	if err != nil {
		return nil, fmt.Errorf("NewTablePlan error: %w", err)
	}
	indexes, err := mdm.IndexInfo(tableName, tx)
	if err != nil {
		return nil, fmt.Errorf("mdm.IndexInfo error: %w", err)
	}
	return &TablePlanner{
		myPlan:  myPlan,
		myPred:  myPred,
		mySche:  *myPlan.Schema(),
		indexes: indexes,
		tx:      tx,
	}, nil
}

func (tp *TablePlanner) MakeSelectPlan() Plan {
	p, ok := tp.MakeIndexSelect()
	if !ok {
		p = tp.myPlan
	}
	return tp.addSelectPred(p)
}

func (tp *TablePlanner) MakeJoinPlan(current Plan) (Plan, bool) {
	currentSche := current.Schema()
	_, ok := tp.myPred.JoinSubPred(&tp.mySche, currentSche)
	if !ok {
		return nil, false
	}
	p, ok := tp.MakeIndexJoin(current, currentSche)
	if !ok {
		p = tp.makeProductJoin(current, currentSche)
	}
	return p, true
}

func (tp *TablePlanner) MakeProductPlan(current Plan) Plan {
	p := tp.addSelectPred(tp.myPlan)
	return multibuffer.NewProductPlan(tp.tx, p, current)
}

func (tp *TablePlanner) MakeIndexSelect() (Plan, bool) {
	for fieldName, indexInfo := range tp.indexes {
		val, ok := tp.myPred.EquatesWithConstant(fieldName)
		if ok {
			return NewIndexSelectPlan(tp.myPlan, &indexInfo, val), true
		}
	}
	return nil, false
}

func (tp *TablePlanner) MakeIndexJoin(current Plan, currentSche *schema.Schema) (Plan, bool) {
	for fieldName, indexInfo := range tp.indexes {
		outerField, ok := tp.myPred.EquatesWithField(fieldName)
		if ok && currentSche.HasField(outerField) {
			p := NewIndexJoinPlan(current, tp.myPlan, &indexInfo, outerField)
			return tp.addJoinPred(tp.addSelectPred(p), currentSche), true
		}
	}
	return nil, false
}

func (tp *TablePlanner) makeProductJoin(current Plan, currentSche *schema.Schema) Plan {
	p := tp.MakeProductPlan(current)
	return tp.addJoinPred(p, currentSche)
}

func (tp *TablePlanner) addSelectPred(p Plan) Plan {
	selectPred, ok := tp.myPred.SelectSubPred(&tp.mySche)
	if ok {
		return NewSelectPlan(p, selectPred)
	}
	return p
}

func (tp *TablePlanner) addJoinPred(p Plan, currentSche *schema.Schema) Plan {
	joinPred, ok := tp.myPred.JoinSubPred(&tp.mySche, currentSche)
	if ok {
		return NewSelectPlan(p, joinPred)
	}
	return p
}
