package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/parse"
	"github.com/abekoh/simple-db/internal/query"
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

type BasicQueryPlanner struct {
	mdm *metadata.Manager
}

func NewBasicQueryPlanner(mdm *metadata.Manager) *BasicQueryPlanner {
	return &BasicQueryPlanner{mdm: mdm}
}

var _ QueryPlanner = (*BasicQueryPlanner)(nil)

func (bp *BasicQueryPlanner) CreatePlan(d *parse.QueryData, tx *transaction.Transaction) (Plan, error) {
	plans := make([]Plan, 0, len(d.Tables()))
	for _, t := range d.Tables() {
		viewDef, ok, err := bp.mdm.ViewDef(t, tx)
		if err != nil {
			return nil, fmt.Errorf("view  error: %w", err)
		}
		if ok {
			p := parse.NewParser(viewDef)
			viewData, err := p.Query()
			if err != nil {
				return nil, fmt.Errorf("view query error: %w", err)
			}
			vp, err := bp.CreatePlan(viewData, tx)
			if err != nil {
				return nil, fmt.Errorf("view plan error: %w", err)
			}
			plans = append(plans, vp)
		} else {
			tablePlan, err := NewTablePlan(t, tx, bp.mdm)
			if err != nil {
				return nil, fmt.Errorf("table plan error: %w", err)
			}
			plans = append(plans, tablePlan)
		}
	}

	plan := plans[0]
	var err error
	for _, p := range plans[1:] {
		plan, err = NewProductPlan(plan, p)
		if err != nil {
			return nil, fmt.Errorf("product plan error: %w", err)
		}
	}

	plan = NewSelectPlan(plan, d.Predicate())

	plan = NewProjectPlan(plan, d.Fields())

	return plan, nil
}

type BasicUpdatePlanner struct {
	mdm *metadata.Manager
}

func NewBasicUpdatePlanner(mdm *metadata.Manager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm: mdm}
}

var _ UpdatePlanner = (*BasicUpdatePlanner)(nil)

func (up *BasicUpdatePlanner) ExecuteInsert(d *parse.InsertData, tx *transaction.Transaction) (int, error) {
	plan, err := NewTablePlan(d.Table(), tx, up.mdm)
	if err != nil {
		return 0, fmt.Errorf("table plan error: %w", err)
	}
	s, err := plan.Open()
	if err != nil {
		return 0, fmt.Errorf("open error: %w", err)
	}
	updateScan, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("table is not updateable")
	}
	if err := updateScan.Insert(); err != nil {
		return 0, fmt.Errorf("insert error: %w", err)
	}
	panic("aa")
}

func (up *BasicUpdatePlanner) ExecuteDelete(d *parse.DeleteData, tx *transaction.Transaction) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (up *BasicUpdatePlanner) ExecuteModify(d *parse.ModifyData, tx *transaction.Transaction) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(d *parse.CreateTableData, tx *transaction.Transaction) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (up *BasicUpdatePlanner) ExecuteCreateView(d *parse.CreateViewData, tx *transaction.Transaction) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (up *BasicUpdatePlanner) ExecuteCreateIndex(d *parse.CreateIndexData, tx *transaction.Transaction) (int, error) {
	//TODO implement me
	panic("implement me")
}
