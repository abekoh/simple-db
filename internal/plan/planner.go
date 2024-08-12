package plan

import (
	"fmt"

	"github.com/abekoh/simple-db/internal/index"
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

type (
	QueryPlannerInitializer  func(*metadata.Manager) QueryPlanner
	UpdatePlannerInitializer func(*metadata.Manager) UpdatePlanner
)

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
	case *parse.BeginData:
		return CommandResult{Type: Begin}, nil
	case *parse.CommitData:
		return CommandResult{Type: Commit}, nil
	case *parse.RollbackData:
		return CommandResult{Type: Rollback}, nil
	case *parse.QueryData:
		r, err := p.qp.CreatePlan(c, tx)
		return r, err
	case *parse.ExplainQueryData:
		p, err := p.qp.CreatePlan(c.QueryData, tx)
		if err != nil {
			return nil, fmt.Errorf("create plan error: %w", err)
		}
		return &ExplainPlan{Plan: p}, nil
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

func NewBasicQueryPlanner(mdm *metadata.Manager) QueryPlanner {
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
	return NewMultiBufferProductPlan(tp.tx, p, current)
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

func NewHeuristicQueryPlanner(mdm *metadata.Manager) QueryPlanner {
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

	// Step4 group
	if d.GroupFields() != nil && d.AggregationFuncs() != nil {
		currentPlan = NewGroupByPlan(tx, currentPlan, d.GroupFields(), d.AggregationFuncs())
	}

	// Step5 sort
	if d.Order() != nil {
		currentPlan = NewSortPlan(tx, currentPlan, d.Order())
	}

	return NewProjectPlan(currentPlan, d.Fields()), nil
}

type BasicUpdatePlanner struct {
	mdm *metadata.Manager
}

func NewBasicUpdatePlanner(mdm *metadata.Manager) UpdatePlanner {
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

type IndexUpdatePlanner struct {
	mdm *metadata.Manager
}

func NewIndexUpdatePlanner(mdm *metadata.Manager) UpdatePlanner {
	return &IndexUpdatePlanner{mdm: mdm}
}

var _ UpdatePlanner = (*IndexUpdatePlanner)(nil)

func (up IndexUpdatePlanner) ExecuteInsert(d *parse.InsertData, tx *transaction.Transaction) (int, error) {
	p, err := NewTablePlan(d.Table(), tx, up.mdm)
	if err != nil {
		return 0, fmt.Errorf("table plan error: %w", err)
	}
	s, err := p.Open()
	if err != nil {
		return 0, fmt.Errorf("open error: %w", err)
	}
	us, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("table is not updateable")
	}
	if err := us.Insert(); err != nil {
		return 0, fmt.Errorf("insert error: %w", err)
	}
	rid := us.RID()

	indexes, err := up.mdm.IndexInfo(d.Table(), tx)
	if err != nil {
		return 0, fmt.Errorf("index info error: %w", err)
	}
	for i, fieldName := range d.Fields() {
		val := d.Values()[i]
		if err := us.SetVal(fieldName, val); err != nil {
			return 0, fmt.Errorf("set value error: %w", err)
		}
		if idxInfo, ok := indexes[fieldName]; ok {
			idx, err := idxInfo.Open()
			if err != nil {
				return 0, fmt.Errorf("index open error: %w", err)
			}
			if err := idx.Insert(val, rid); err != nil {
				return 0, fmt.Errorf("index insert error: %w", err)
			}
			if err := idx.Close(); err != nil {
				return 0, fmt.Errorf("index close error: %w", err)
			}
		}
	}
	if err := us.Close(); err != nil {
		return 0, fmt.Errorf("close error: %w", err)
	}
	return 1, nil
}

func (up IndexUpdatePlanner) ExecuteDelete(d *parse.DeleteData, tx *transaction.Transaction) (int, error) {
	tp, err := NewTablePlan(d.Table(), tx, up.mdm)
	if err != nil {
		return 0, fmt.Errorf("table plan error: %w", err)
	}
	sp := NewSelectPlan(tp, d.Predicate())
	indexes, err := up.mdm.IndexInfo(d.Table(), tx)
	if err != nil {
		return 0, fmt.Errorf("index info error: %w", err)
	}

	s, err := sp.Open()
	if err != nil {
		return 0, fmt.Errorf("open error: %w", err)
	}
	us, ok := s.(query.UpdateScan)
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
		rid := us.RID()
		for fieldName, idxInfo := range indexes {
			val, err := us.Val(fieldName)
			if err != nil {
				return 0, fmt.Errorf("val error: %w", err)
			}
			idx, err := idxInfo.Open()
			if err != nil {
				return 0, fmt.Errorf("index open error: %w", err)
			}
			if err := idx.Delete(val, rid); err != nil {
				return 0, fmt.Errorf("index delete error: %w", err)
			}
			if err := idx.Close(); err != nil {
				return 0, fmt.Errorf("index close error: %w", err)
			}
		}
		if err := us.Delete(); err != nil {
			return 0, fmt.Errorf("delete error: %w", err)
		}
		count++
	}
	if err := s.Close(); err != nil {
		return 0, fmt.Errorf("close error: %w", err)
	}
	return count, nil
}

func (up IndexUpdatePlanner) ExecuteModify(d *parse.ModifyData, tx *transaction.Transaction) (int, error) {
	tp, err := NewTablePlan(d.Table(), tx, up.mdm)
	if err != nil {
		return 0, fmt.Errorf("table plan error: %w", err)
	}
	sp := NewSelectPlan(tp, d.Predicate())

	indexes, err := up.mdm.IndexInfo(d.Table(), tx)
	if err != nil {
		return 0, fmt.Errorf("index info error: %w", err)
	}
	var idx index.Index
	idxInfo, ok := indexes[d.Field()]
	if ok {
		idx, err = idxInfo.Open()
		if err != nil {
			return 0, fmt.Errorf("index open error: %w", err)
		}
	}

	s, err := sp.Open()
	if err != nil {
		return 0, fmt.Errorf("open error: %w", err)
	}
	us, ok := s.(query.UpdateScan)
	if !ok {
		return 0, fmt.Errorf("table is not updateable")
	}
	count := 0
	for {
		ok, err := us.Next()
		if err != nil {
			return 0, fmt.Errorf("next error: %w", err)
		}
		if !ok {
			break
		}
		newVal, err := d.Value().Evaluate(us)
		if err != nil {
			return 0, fmt.Errorf("evaluate error: %w", err)
		}
		oldVal, err := us.Val(d.Field())
		if err != nil {
			return 0, fmt.Errorf("val error: %w", err)
		}
		if err := us.SetVal(d.Field(), newVal); err != nil {
			return 0, fmt.Errorf("set value error: %w", err)
		}
		if idx != nil {
			rid := us.RID()
			if err := idx.Delete(oldVal, rid); err != nil {
				return 0, fmt.Errorf("index delete error: %w", err)
			}
			if err := idx.Insert(newVal, rid); err != nil {
				return 0, fmt.Errorf("index insert error: %w", err)
			}
		}
		count++
	}
	if idx != nil {
		if err := idx.Close(); err != nil {
			return 0, fmt.Errorf("index close error: %w", err)
		}
	}
	if err := s.Close(); err != nil {
		return 0, fmt.Errorf("close error: %w", err)
	}
	return count, nil
}

func (up IndexUpdatePlanner) ExecuteCreateTable(d *parse.CreateTableData, tx *transaction.Transaction) (int, error) {
	if err := up.mdm.CreateTable(d.Table(), d.Schema(), tx); err != nil {
		return 0, fmt.Errorf("create table error: %w", err)
	}
	return 0, nil
}

func (up IndexUpdatePlanner) ExecuteCreateView(d *parse.CreateViewData, tx *transaction.Transaction) (int, error) {
	if err := up.mdm.CreateView(d.ViewName(), d.ViewDef(), tx); err != nil {
		return 0, fmt.Errorf("create view error: %w", err)
	}
	return 0, nil
}

func (up IndexUpdatePlanner) ExecuteCreateIndex(d *parse.CreateIndexData, tx *transaction.Transaction) (int, error) {
	if err := up.mdm.CreateIndex(d.Index(), d.Table(), d.Field(), tx); err != nil {
		return 0, fmt.Errorf("create index error: %w", err)
	}
	return 0, nil
}
