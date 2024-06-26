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

func (p *Planner) Execute(q string, tx *transaction.Transaction) (Result, error) {
	ps := parse.NewParser(q)
	d, err := ps.ToData()
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	switch c := d.(type) {
	case *parse.QueryData:
		r, err := p.qp.CreatePlan(c, tx)
		return r, err
	case *parse.InsertData:
		r, err := p.up.ExecuteInsert(c, tx)
		return CommandResult(r), err
	case *parse.DeleteData:
		r, err := p.up.ExecuteDelete(c, tx)
		return CommandResult(r), err
	case *parse.ModifyData:
		r, err := p.up.ExecuteModify(c, tx)
		return CommandResult(r), err
	case *parse.CreateTableData:
		r, err := p.up.ExecuteCreateTable(c, tx)
		return CommandResult(r), err
	case *parse.CreateViewData:
		r, err := p.up.ExecuteCreateView(c, tx)
		return CommandResult(r), err
	case *parse.CreateIndexData:
		//return p.up.ExecuteCreateIndex(c, tx)
		r, err := p.up.ExecuteCreateIndex(c, tx)
		return CommandResult(r), err
	}
	return nil, fmt.Errorf("unknown command type %v", d)
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
	d, err := ps.ToData()
	if err != nil {
		return 0, fmt.Errorf("parse error: %w", err)
	}
	switch c := d.(type) {
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
	return 0, fmt.Errorf("unknown command type %v", d)
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
	for i, f := range d.Fields() {
		if len(d.Values()) <= i {
			return 0, fmt.Errorf("field and value count mismatch")
		}
		if err := updateScan.SetVal(f, d.Values()[i]); err != nil {
			return 0, fmt.Errorf("set value error: %w", err)
		}
	}
	if err := s.Close(); err != nil {
		return 0, fmt.Errorf("close error: %w", err)
	}
	return 1, nil
}

func (up *BasicUpdatePlanner) ExecuteDelete(d *parse.DeleteData, tx *transaction.Transaction) (int, error) {
	var plan Plan
	plan, err := NewTablePlan(d.Table(), tx, up.mdm)
	if err != nil {
		return 0, fmt.Errorf("table plan error: %w", err)
	}
	plan = NewSelectPlan(plan, d.Predicate())
	s, err := plan.Open()
	if err != nil {
		return 0, fmt.Errorf("open error: %w", err)
	}
	updateScan, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("table is not updateable")
	}
	count := 0
	for {
		ok, err := s.Next()
		if err != nil {
			return 0, fmt.Errorf("next error: %w", err)
		}
		if !ok {
			break
		}
		if err := updateScan.Delete(); err != nil {
			return 0, fmt.Errorf("delete error: %w", err)
		}
		count++
	}
	if err := s.Close(); err != nil {
		return 0, fmt.Errorf("close error: %w", err)
	}
	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteModify(d *parse.ModifyData, tx *transaction.Transaction) (int, error) {
	var plan Plan
	plan, err := NewTablePlan(d.Table(), tx, up.mdm)
	if err != nil {
		return 0, fmt.Errorf("table plan error: %w", err)
	}
	plan = NewSelectPlan(plan, d.Predicate())
	s, err := plan.Open()
	if err != nil {
		return 0, fmt.Errorf("open error: %w", err)
	}
	updateScan, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("table is not updateable")
	}
	count := 0
	for {
		ok, err := updateScan.Next()
		if err != nil {
			return 0, fmt.Errorf("next error: %w", err)
		}
		if !ok {
			break
		}
		val, err := d.Value().Evaluate(updateScan)
		if err != nil {
			return 0, fmt.Errorf("evaluate error: %w", err)
		}
		if err := updateScan.SetVal(d.Field(), val); err != nil {
			return 0, fmt.Errorf("set value error: %w", err)
		}
		count++
	}
	if err := s.Close(); err != nil {
		return 0, fmt.Errorf("close error: %w", err)
	}
	return count, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(d *parse.CreateTableData, tx *transaction.Transaction) (int, error) {
	if err := up.mdm.CreateTable(d.Table(), d.Schema(), tx); err != nil {
		return 0, fmt.Errorf("create table error: %w", err)
	}
	return 0, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateView(d *parse.CreateViewData, tx *transaction.Transaction) (int, error) {
	if err := up.mdm.CreateView(d.ViewName(), d.ViewDef(), tx); err != nil {
		return 0, fmt.Errorf("create view error: %w", err)
	}
	return 0, nil
}

func (up *BasicUpdatePlanner) ExecuteCreateIndex(d *parse.CreateIndexData, tx *transaction.Transaction) (int, error) {
	if err := up.mdm.CreateIndex(d.Index(), d.Table(), d.Field(), tx); err != nil {
		return 0, fmt.Errorf("create index error: %w", err)
	}
	return 0, nil
}
