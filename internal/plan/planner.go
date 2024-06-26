package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/metadata"
	"github.com/abekoh/simple-db/internal/parse"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
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
	qp  QueryPlanner
	up  UpdatePlanner
	mdm *metadata.Manager
}

func NewPlanner(qp QueryPlanner, up UpdatePlanner, mdm *metadata.Manager) *Planner {
	return &Planner{qp: qp, up: up, mdm: mdm}
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
		return CommandResult{Type: Insert, Count: r}, err
	case *parse.DeleteData:
		r, err := p.up.ExecuteDelete(c, tx)
		return CommandResult{Type: Delete, Count: r}, err
	case *parse.ModifyData:
		r, err := p.up.ExecuteModify(c, tx)
		return CommandResult{Type: Update, Count: r}, err
	case *parse.CreateTableData:
		r, err := p.up.ExecuteCreateTable(c, tx)
		return CommandResult{Type: CreateTable, Count: r}, err
	case *parse.CreateViewData:
		r, err := p.up.ExecuteCreateView(c, tx)
		return CommandResult{Type: CreateView, Count: r}, err
	case *parse.CreateIndexData:
		r, err := p.up.ExecuteCreateIndex(c, tx)
		return CommandResult{Type: CreateIndex, Count: r}, err
	}
	return nil, fmt.Errorf("unknown command type %v", d)
}

func (p *Planner) Prepare(q string, tx *transaction.Transaction) (statement.Prepared, error) {
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
		return c, nil
	case *parse.ModifyData:
		return c, nil
	case *parse.DeleteData:
		return c, nil
	}
	return nil, fmt.Errorf("unknown command type %v", d)
}

func (p *Planner) Bind(pre statement.Prepared, rawParams map[int]any, tx *transaction.Transaction) (statement.Bound, error) {
	placeholders := pre.Placeholders(func(tableName string) (*schema.Schema, error) {
		l, err := p.mdm.Layout(tableName, tx)
		if err != nil {
			return nil, fmt.Errorf("layout error: %w", err)
		}
		return l.Schema(), nil
	})
	params := make(map[int]schema.Constant)
	for k, v := range rawParams {
		fieldType, ok := placeholders[k]
		if !ok {
			return nil, fmt.Errorf("missing placeholder: %d", k)
		}
		switch fieldType {
		case schema.Varchar:
			params[k] = schema.ConstantStr(v.(string))
		case schema.Integer32:
			params[k] = schema.ConstantInt32(v.(int32))
		default:
			return nil, fmt.Errorf("unsupported field type: %v", fieldType)
		}
	}
	bound, err := pre.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("swap params error: %w", err)
	}
	return bound, nil
}

func (p *Planner) ExecuteBound(bound statement.Bound, tx *transaction.Transaction) (Result, error) {
	switch b := bound.(type) {
	case Plan:
		return b, nil
	case *parse.BoundInsertData:
		r, err := p.up.ExecuteInsert(&b.InsertData, tx)
		return CommandResult{Type: Insert, Count: r}, err
	case *parse.BoundModifyData:
		r, err := p.up.ExecuteModify(&b.ModifyData, tx)
		return CommandResult{Type: Update, Count: r}, err
	case *parse.BoundDeleteData:
		r, err := p.up.ExecuteDelete(&b.DeleteData, tx)
		return CommandResult{Type: Delete, Count: r}, err
	}
	return nil, fmt.Errorf("unknown bound type %v", bound)
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
