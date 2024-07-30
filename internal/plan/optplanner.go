package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/metadata"
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

func (p *TablePlanner) MakeIndexPlan(current Plan) Plan {
	p
}

func (p *TablePlanner) addSelectPred(p Plan) (Plan, error) {
	selectPred := p.myPred.se
}
