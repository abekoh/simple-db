package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/parse"
	"github.com/abekoh/simple-db/internal/transaction"
)

type QueryPlanner interface {
	CreatePlan(d *parse.QueryData, tx *transaction.Transaction) (Plan, error)
}

type UpdatePlanner interface {
	ExecuteInsert(d *parse.InsertData, tx *transaction.Transaction) (int, error)
	ExecuteDelete(d *parse.DeleteData, tx *transaction.Transaction) (int, error)
	ExecuteModify(d *parse.ModifyData, tx *transaction.Transaction) (int, error)
	ExecuteCreateTable(d *parse.CreateTableData, tx *transaction.Transaction) (int, error)
	ExecuteCreateView(d *parse.CreateViewData, tx *transaction.Transaction) (int, error)
	ExecuteCreateIndex(d *parse.CreateIndexData, tx *transaction.Transaction) (int, error)
}

type Planner struct {
	qp QueryPlanner
	up UpdatePlanner
}

func NewPlanner(qp QueryPlanner, up UpdatePlanner) *Planner {
	return &Planner{qp: qp, up: up}
}

func (p *Planner) CreateQueryPlan(q string, tx *transaction.Transaction) (Plan, error) {
	ps := parse.NewParser(q)
	qd, err := ps.Query()
	if err != nil {
		return nil, err
	}
	return p.qp.CreatePlan(qd, tx)
}

func (p *Planner) ExecuteUpdate(q string, tx *transaction.Transaction) (int, error) {
	ps := parse.NewParser(q)
	cmd, err := ps.UpdateCommand()
	if err != nil {
		return 0, err
	}
	switch c := cmd.(type) {
	case *parse.InsertData:
		return p.up.ExecuteInsert(c, tx)
	case *parse.DeleteData:
		return p.up.ExecuteDelete(c, tx)
	case *parse.ModifyData:
		return p.up.ExecuteModify(c, tx)
	case *parse.CreateTableData:
		return p.up.ExecuteCreateTable(c, tx)
	case *parse.CreateViewData:
		return p.up.ExecuteCreateView(c, tx)
	case *parse.CreateIndexData:
		return p.up.ExecuteCreateIndex(c, tx)
	}
	return 0, fmt.Errorf("unknown command type %v", cmd)
}
