package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/multibuffer"
	"github.com/abekoh/simple-db/internal/parse"
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

type HeuristicQueryPlanner struct {
	tablePlanners []TablePlanner
	mdm           *metadata.Manager
}

var _ QueryPlanner = (*HeuristicQueryPlanner)(nil)

func NewHeuristicQueryPlanner(mdm *metadata.Manager) *HeuristicQueryPlanner {
	return &HeuristicQueryPlanner{mdm: mdm}
}

func (h HeuristicQueryPlanner) CreatePlan(d *parse.QueryData, tx *transaction.Transaction) (Plan, error) {
	// Step1
	tablePlanners := make([]TablePlanner, len(d.Tables()))
	for i, tableName := range d.Tables() {
		tp, err := NewTablePlanner(tableName, d.Predicate(), tx, h.mdm)
		if err != nil {
			return nil, fmt.Errorf("NewTablePlanner error: %w", err)
		}
		tablePlanners[i] = *tp
	}

	var currentPlan Plan

	// Step2
	var (
		lowestSelectPlan Plan
		lowestSelectIdx  int
	)
	for i, tp := range tablePlanners {
		p := tp.MakeSelectPlan()
		if lowestSelectPlan == nil ||
			p.RecordsOutput() < lowestSelectPlan.RecordsOutput() {
			lowestSelectPlan = p
			lowestSelectIdx = i
		}
	}
	tablePlanners = append(tablePlanners[:lowestSelectIdx], tablePlanners[lowestSelectIdx+1:]...)
	currentPlan = lowestSelectPlan

	// Step3
	for len(tablePlanners) > 0 {
		var (
			lowestJoinPlan Plan
			lowestJoinIdx  int
		)
		for i, tp := range tablePlanners {
			p, ok := tp.MakeJoinPlan(lowestSelectPlan)
			if ok && (lowestJoinPlan == nil ||
				p.RecordsOutput() < lowestJoinPlan.RecordsOutput()) {
				lowestJoinPlan = p
				lowestJoinIdx = i
			}
			tablePlanners = append(tablePlanners[:lowestJoinIdx], tablePlanners[lowestJoinIdx+1:]...)
		}
		if lowestJoinPlan != nil {
			currentPlan = lowestJoinPlan
		} else {
			var (
				lowestProductPlan Plan
				lowestProductIdx  int
			)
			for i, tp := range tablePlanners {
				p := tp.MakeProductPlan(currentPlan)
				if lowestProductPlan == nil ||
					p.RecordsOutput() < lowestProductPlan.RecordsOutput() {
					lowestProductPlan = p
					lowestProductIdx = i
				}
			}
			tablePlanners = append(tablePlanners[:lowestProductIdx], tablePlanners[lowestProductIdx+1:]...)
			currentPlan = lowestProductPlan
		}
	}

	return NewProjectPlan(currentPlan, d.Fields()), nil
}
