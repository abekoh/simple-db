package parse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/statement"
)

type Parser struct {
	lexer *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{lexer: NewLexer(s)}
}

func (p *Parser) selectOne() (schema.FieldName, query.AggregationFunc, token, error) {
	tok := p.lexer.NextToken()
	switch tok.typ {
	case sum:
		alias, aggregationFunc, tok2, err := p.aggregation(query.NewSumFunc)
		if err != nil {
			return "", nil, tok, fmt.Errorf("failed to parse sum aggregation: %w", err)
		}
		return alias, aggregationFunc, tok2, nil
	case max:
		alias, aggregationFunc, tok2, err := p.aggregation(query.NewMaxFunc)
		if err != nil {
			return "", nil, tok, fmt.Errorf("failed to parse max aggregation: %w", err)
		}
		return alias, aggregationFunc, tok2, nil
	case min:
		alias, aggregationFunc, tok2, err := p.aggregation(query.NewMinFunc)
		if err != nil {
			return "", nil, tok, fmt.Errorf("failed to parse min aggregation: %w", err)
		}
		return alias, aggregationFunc, tok2, nil
	case count:
		alias, aggregationFunc, tok2, err := p.aggregation(query.NewCountFunc)
		if err != nil {
			return "", nil, tok, fmt.Errorf("failed to parse count aggregation: %w", err)
		}
		return alias, aggregationFunc, tok2, nil
	case identifier:
		return schema.FieldName(tok.literal), nil, tok, nil
	default:
		return "", nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
}

func (p *Parser) aggregation(initializer query.AggregationFuncInitializer) (schema.FieldName, query.AggregationFunc, token, error) {
	tok := p.lexer.NextToken()
	if tok.typ != lparen {
		return "", nil, tok, fmt.Errorf("expected (, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return "", nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	aggFuncIdent := tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != rparen {
		return "", nil, tok, fmt.Errorf("expected ), got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != as {
		return "", nil, tok, fmt.Errorf("expected AS, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return "", nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	alias := tok.literal
	return schema.FieldName(alias), initializer(schema.FieldName(aggFuncIdent), schema.FieldName(alias)), tok, nil
}

func (p *Parser) selectList() ([]schema.FieldName, []query.AggregationFunc, token, error) {
	fields := make([]schema.FieldName, 0, 1)
	var aggregationFuncs []query.AggregationFunc
	field, aggregationFunc, tok, err := p.selectOne()
	if err != nil {
		return nil, nil, tok, fmt.Errorf("failed to parse select one: %w", err)
	}
	fields = append(fields, field)
	if aggregationFunc != nil {
		aggregationFuncs = append(aggregationFuncs, aggregationFunc)
	}
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		field, aggregationFunc, tok, err := p.selectOne()
		if err != nil {
			return nil, nil, tok, fmt.Errorf("failed to parse select one: %w", err)
		}
		fields = append(fields, field)
		if aggregationFunc != nil {
			aggregationFuncs = append(aggregationFuncs, aggregationFunc)
		}
	}
	return fields, aggregationFuncs, tok, nil
}

func (p *Parser) tableList() ([]string, token, error) {
	tables := make([]string, 0, 1)
	tok := p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	tables = append(tables, tok.literal)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		tables = append(tables, tok.literal)
	}
	return tables, tok, nil
}

func (p *Parser) join() ([]string, query.Predicate, token, error) {
	tables := make([]string, 0, 1)
	tok := p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	tables = append(tables, tok.literal)

	tok = p.lexer.NextToken()
	if tok.typ != on {
		return nil, nil, tok, fmt.Errorf("expected ON, got %s", tok.literal)
	}
	pred := make(query.Predicate, 0, 1)
	t, tok, err := p.term()
	if err != nil {
		return nil, nil, tok, fmt.Errorf("failed to parse term: %w", err)
	}
	pred = append(pred, t)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != and {
			break
		}
		t, tok, err := p.term()
		if err != nil {
			return nil, nil, tok, fmt.Errorf("failed to parse term: %w", err)
		}
		pred = append(pred, t)
	}
	return tables, pred, tok, nil
}

func (p *Parser) fieldList() ([]schema.FieldName, token, error) {
	fields := make([]schema.FieldName, 0, 1)
	tok := p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	fields = append(fields, schema.FieldName(tok.literal))
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		fields = append(fields, schema.FieldName(tok.literal))
	}
	return fields, tok, nil
}

func (p *Parser) constList() ([]schema.Constant, token, error) {
	constList := make([]schema.Constant, 0, 1)
	c, tok, err := p.constant()
	if err != nil {
		return nil, tok, fmt.Errorf("failed to parse constant: %w", err)
	}
	constList = append(constList, c)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		c, tok, err := p.constant()
		if err != nil {
			return nil, tok, fmt.Errorf("failed to parse constant: %w", err)
		}
		constList = append(constList, c)
	}
	return constList, tok, nil
}

func (p *Parser) predicate() (query.Predicate, token, error) {
	terms := make([]query.Term, 0, 1)
	var tok token
	for {
		term, tok, err := p.term()
		if err != nil {
			return nil, tok, fmt.Errorf("failed to parse term: %w", err)
		}
		terms = append(terms, term)
		tok = p.lexer.NextToken()
		if tok.typ != and {
			break
		}
	}
	return query.NewPredicate(terms...), tok, nil
}

func (p *Parser) term() (query.Term, token, error) {
	lhs, tok, err := p.expression()
	if err != nil {
		return query.Term{}, tok, fmt.Errorf("failed to parse expression: %w", err)
	}
	tok = p.lexer.NextToken()
	if tok.typ != equal {
		return query.Term{}, tok, fmt.Errorf("expected =, got %s", tok.literal)
	}
	rhs, tok, err := p.expression()
	if err != nil {
		return query.Term{}, tok, fmt.Errorf("failed to parse expression: %w", err)
	}
	return query.NewTerm(lhs, rhs), tok, nil
}

func (p *Parser) expression() (query.Expression, token, error) {
	tok := p.lexer.NextToken()
	switch tok.typ {
	case identifier:
		return schema.FieldName(tok.literal), tok, nil
	case number:
		i, err := strconv.Atoi(tok.literal)
		if err != nil {
			return nil, tok, fmt.Errorf("failed to parse number: %w", err)
		}
		return schema.ConstantInt32(i), tok, nil
	case stringTok:
		return schema.ConstantStr(tok.literal), tok, nil
	case placeholder:
		i, err := strconv.Atoi(tok.literal[1:])
		if err != nil {
			return nil, tok, fmt.Errorf("failed to parse number: %w", err)
		}
		return schema.Placeholder(i), tok, nil
	}
	return nil, tok, fmt.Errorf("unexpected token %s", tok.literal)
}

func (p *Parser) constant() (schema.Constant, token, error) {
	tok := p.lexer.NextToken()
	switch tok.typ {
	case number:
		i, err := strconv.Atoi(tok.literal)
		if err != nil {
			return nil, tok, fmt.Errorf("failed to parse number: %w", err)
		}
		return schema.ConstantInt32(i), tok, nil
	case stringTok:
		return schema.ConstantStr(tok.literal), tok, nil
	case placeholder:
		i, err := strconv.Atoi(tok.literal[1:])
		if err != nil {
			return nil, tok, fmt.Errorf("failed to parse number: %w", err)
		}
		return schema.Placeholder(i), tok, nil
	}
	return nil, tok, fmt.Errorf("unexpected token %s", tok.literal)
}

func (p *Parser) fieldDef() (schema.FieldName, schema.Field, token, error) {
	tok := p.lexer.NextToken()
	if tok.typ != identifier {
		return "", schema.Field{}, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	fieldName := schema.FieldName(tok.literal)
	tok = p.lexer.NextToken()
	switch tok.typ {
	case intTok:
		return fieldName, schema.NewInt32Field(), tok, nil
	case varchar:
		tok = p.lexer.NextToken()
		if tok.typ != lparen {
			return "", schema.Field{}, tok, fmt.Errorf("expected (, got %s", tok.literal)
		}
		tok = p.lexer.NextToken()
		if tok.typ != number {
			return "", schema.Field{}, tok, fmt.Errorf("expected number, got %s", tok.literal)
		}
		i, err := strconv.Atoi(tok.literal)
		if err != nil {
			return "", schema.Field{}, tok, fmt.Errorf("failed to parse number: %w", err)
		}
		tok = p.lexer.NextToken()
		if tok.typ != rparen {
			return "", schema.Field{}, tok, fmt.Errorf("expected ), got %s", tok.literal)
		}
		return fieldName, schema.NewVarcharField(int32(i)), tok, nil
	}
	return "", schema.Field{}, tok, fmt.Errorf("unexpected token %s", tok.literal)
}

func (p *Parser) fieldDefs() (schema.Schema, token, error) {
	s := schema.NewSchema()
	var tk token
	for {
		fieldName, f, tok, err := p.fieldDef()
		if err != nil {
			return schema.Schema{}, tok, fmt.Errorf("failed to parse field definition: %w", err)
		}
		s.AddField(fieldName, f)
		tk = p.lexer.NextToken()
		if tk.typ != comma {
			break
		}
	}
	return s, tk, nil
}

func (p *Parser) order() (query.Order, token, error) {
	order := make(query.Order, 0, 1)
	tok := p.lexer.NextToken()
	if tok.typ != by {
		return nil, tok, fmt.Errorf("expected BY, got %s", tok.literal)
	}
	for {
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		field := schema.FieldName(tok.literal)
		tok = p.lexer.NextToken()
		switch tok.typ {
		case asc:
			order = append(order, query.OrderElement{Field: field, OrderType: query.Asc})
			tok = p.lexer.NextToken()
		case desc:
			order = append(order, query.OrderElement{Field: field, OrderType: query.Desc})
			tok = p.lexer.NextToken()
		default:
			order = append(order, query.OrderElement{Field: field, OrderType: query.Asc})
		}
		if tok.typ != comma {
			break
		}
	}
	return order, tok, nil
}

func (p *Parser) group() ([]schema.FieldName, token, error) {
	tok := p.lexer.NextToken()
	if tok.typ != by {
		return nil, tok, fmt.Errorf("expected BY, got %s", tok.literal)
	}
	groupFields := make([]schema.FieldName, 0, 1)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		groupFields = append(groupFields, schema.FieldName(tok.literal))
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
	}
	return groupFields, tok, nil
}

type Data interface {
	Data()
}

type InsertData struct {
	table  string
	fields []schema.FieldName
	values []schema.Constant
}

type BoundInsertData struct {
	InsertData
}

func (d BoundInsertData) Bound() {}

func (d InsertData) Data() {}

func (d InsertData) Table() string {
	return d.table
}

func (d InsertData) Fields() []schema.FieldName {
	return d.fields
}

func (d InsertData) Values() []schema.Constant {
	return d.values
}

func (p *Parser) Insert() (*InsertData, error) {
	d := &InsertData{}
	tok := p.lexer.NextToken()
	if tok.typ != insert {
		return nil, fmt.Errorf("expected INSERT, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != into {
		return nil, fmt.Errorf("expected INTO, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.table = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != lparen {
		return nil, fmt.Errorf("expected (, got %s", tok.literal)
	}
	fieldList, tok, err := p.fieldList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse field list: %w", err)
	}
	d.fields = fieldList
	if tok.typ != rparen {
		return nil, fmt.Errorf("expected ), got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != values {
		return nil, fmt.Errorf("expected VALUES, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != lparen {
		return nil, fmt.Errorf("expected (, got %s", tok.literal)
	}
	constList, tok, err := p.constList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse constant list: %w", err)
	}
	d.values = constList
	if tok.typ != rparen {
		return nil, fmt.Errorf("expected ), got %s", tok.literal)
	}
	return d, nil
}

func (d InsertData) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	sche, err := findSchema(d.table)
	if err != nil {
		return nil
	}
	placeholders := make(map[int]schema.FieldType)
	for i, v := range d.values {
		if p, ok := v.(schema.Placeholder); ok {
			placeholders[int(p)] = sche.Typ(d.fields[i])
		}
	}
	return placeholders
}

func (d InsertData) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	values := make([]schema.Constant, len(d.values))
	for i, v := range d.values {
		if p, ok := v.(schema.Placeholder); ok {
			val, ok := params[int(p)]
			if !ok {
				return nil, fmt.Errorf("missing parameter: %d", p)
			}
			values[i] = val
		} else {
			values[i] = v
		}
	}
	return &BoundInsertData{
		InsertData{
			table:  d.table,
			fields: d.fields,
			values: values,
		},
	}, nil
}

type QueryData struct {
	fields           []schema.FieldName
	aggregationFuncs []query.AggregationFunc
	tables           []string
	pred             query.Predicate
	groupFields      []schema.FieldName
	order            query.Order
}

func (d QueryData) Data() {}

func (d QueryData) Fields() []schema.FieldName {
	return d.fields
}

func (d QueryData) Tables() []string {
	return d.tables
}

func (d QueryData) Predicate() query.Predicate {
	return d.pred
}

func (d QueryData) Order() query.Order {
	return d.order
}

func (d QueryData) String() string {
	var b strings.Builder
	b.WriteString("Field:            ")
	b.WriteString(fmt.Sprintf("%v\n", d.fields))
	if len(d.aggregationFuncs) > 0 {
		b.WriteString("AggregationFuncs: ")
		b.WriteString(fmt.Sprintf("%v\n", d.aggregationFuncs))
	}
	b.WriteString("Tables:           ")
	b.WriteString(fmt.Sprintf("%v\n", d.tables))
	if len(d.pred) > 0 {
		b.WriteString("Predicate:        ")
		b.WriteString(fmt.Sprintf("%v\n", d.pred))
	}
	if len(d.groupFields) > 0 {
		b.WriteString("GroupFields:      ")
		b.WriteString(fmt.Sprintf("%v\n", d.groupFields))
	}
	if len(d.order) > 0 {
		b.WriteString("Order:            ")
		b.WriteString(fmt.Sprintf("%v\n", d.order))
	}
	return b.String()
}

func (p *Parser) Query() (*QueryData, error) {
	q := &QueryData{}
	tok := p.lexer.NextToken()
	if tok.typ != selectTok {
		return nil, fmt.Errorf("expected SELECT, got %s", tok.literal)
	}
	selectList, aggregationFuncs, tok, err := p.selectList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse select list: %w", err)
	}
	q.fields = selectList
	q.aggregationFuncs = aggregationFuncs
	if tok.typ != from {
		return nil, fmt.Errorf("expected FROM, got %s", tok.literal)
	}
	tableList, tok, err := p.tableList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse table list: %w", err)
	}
	q.tables = tableList

	for tok.typ == join {
		joinTables, joinPred, tok2, err := p.join()
		if err != nil {
			return nil, fmt.Errorf("failed to parse join: %w", err)
		}
		q.tables = append(q.tables, joinTables...)
		q.pred = append(q.pred, joinPred...)
		tok = tok2
	}

	if tok.typ == where {
		pred, tok2, err := p.predicate()
		if err != nil {
			return nil, fmt.Errorf("failed to parse predicate: %w", err)
		}
		q.pred = append(q.pred, pred...)
		tok = tok2
	}
	if tok.typ == group {
		groupFields, tok2, err := p.group()
		if err != nil {
			return nil, fmt.Errorf("failed to parse group: %w", err)
		}
		q.groupFields = groupFields
		tok = tok2
	}
	if tok.typ == order {
		order, tok2, err := p.order()
		if err != nil {
			return nil, fmt.Errorf("failed to parse order: %w", err)
		}
		q.order = order
		tok = tok2
	}
	return q, nil
}

type ModifyData struct {
	table string
	field schema.FieldName
	value query.Expression
	pred  query.Predicate
}

type BoundModifyData struct {
	ModifyData
}

func (d BoundModifyData) Bound() {}

func (d ModifyData) Table() string {
	return d.table
}

func (d ModifyData) Field() schema.FieldName {
	return d.field
}

func (d ModifyData) Predicate() query.Predicate {
	return d.pred
}

func (d ModifyData) Value() query.Expression {
	return d.value
}

func (d ModifyData) Data() {}

func (p *Parser) Modify() (*ModifyData, error) {
	d := &ModifyData{}
	tok := p.lexer.NextToken()
	if tok.typ != update {
		return nil, fmt.Errorf("expected UPDATE, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.table = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != set {
		return nil, fmt.Errorf("expected SET, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.field = schema.FieldName(tok.literal)
	tok = p.lexer.NextToken()
	if tok.typ != equal {
		return nil, fmt.Errorf("expected =, got %s", tok.literal)
	}
	val, tok, err := p.expression()
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression: %w", err)
	}
	d.value = val
	tok = p.lexer.NextToken()
	if tok.typ == where {
		pred, _, err := p.predicate()
		if err != nil {
			return nil, fmt.Errorf("failed to parse predicate: %w", err)
		}
		d.pred = pred
	}
	return d, nil
}

func (d ModifyData) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	sche, err := findSchema(d.table)
	if err != nil {
		return nil
	}
	placeholders := make(map[int]schema.FieldType)
	if p, ok := d.value.(schema.Placeholder); ok {
		placeholders[int(p)] = sche.Typ(d.field)
	}
	for _, t := range d.pred {
		lhs, rhs := t.Expressions()
		if p, ok := lhs.(schema.Placeholder); ok {
			if fn, ok := rhs.(schema.FieldName); ok {
				placeholders[int(p)] = sche.Typ(fn)
			}
		}
		if p, ok := rhs.(schema.Placeholder); ok {
			if fn, ok := lhs.(schema.FieldName); ok {
				placeholders[int(p)] = sche.Typ(fn)
			}
		}
	}
	return placeholders
}

func (d ModifyData) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	var value query.Expression
	if p, ok := d.value.(schema.Placeholder); ok {
		val, ok := params[int(p)]
		if !ok {
			return nil, fmt.Errorf("missing parameter: %d", p)
		}
		e, ok := val.(query.Expression)
		if !ok {
			return nil, fmt.Errorf("parameter is not an expression: %v", val)
		}
		value = e
	} else {
		value = d.value
	}
	pred, err := d.pred.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("pred.SwapParams error: %w", err)
	}
	return &BoundModifyData{
		ModifyData{
			table: d.table,
			field: d.field,
			value: value,
			pred:  pred,
		},
	}, nil
}

type DeleteData struct {
	table string
	pred  query.Predicate
}

type BoundDeleteData struct {
	DeleteData
}

func (d BoundDeleteData) Bound() {}

func (d DeleteData) Table() string {
	return d.table
}

func (d DeleteData) Predicate() query.Predicate {
	return d.pred
}

func (d DeleteData) Data() {}

func (p *Parser) Delete() (*DeleteData, error) {
	d := &DeleteData{}
	tok := p.lexer.NextToken()
	if tok.typ != deleteTok {
		return nil, fmt.Errorf("expected DELETE, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != from {
		return nil, fmt.Errorf("expected FROM, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.table = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ == where {
		pred, _, err := p.predicate()
		if err != nil {
			return nil, fmt.Errorf("failed to parse predicate: %w", err)
		}
		d.pred = pred
	}
	return d, nil
}

func (d DeleteData) Placeholders(findSchema func(tableName string) (*schema.Schema, error)) map[int]schema.FieldType {
	sche, err := findSchema(d.table)
	if err != nil {
		return nil
	}
	placeholders := make(map[int]schema.FieldType)
	for _, t := range d.pred {
		lhs, rhs := t.Expressions()
		if p, ok := lhs.(schema.Placeholder); ok {
			if fn, ok := rhs.(schema.FieldName); ok {
				placeholders[int(p)] = sche.Typ(fn)
			}
		}
		if p, ok := rhs.(schema.Placeholder); ok {
			if fn, ok := lhs.(schema.FieldName); ok {
				placeholders[int(p)] = sche.Typ(fn)
			}
		}
	}
	return placeholders
}

func (d DeleteData) SwapParams(params map[int]schema.Constant) (statement.Bound, error) {
	pred, err := d.pred.SwapParams(params)
	if err != nil {
		return nil, fmt.Errorf("pred.SwapParams error: %w", err)
	}
	return &BoundDeleteData{
		DeleteData{
			table: d.table,
			pred:  pred,
		},
	}, nil
}

type CreateTableData struct {
	table string
	sche  schema.Schema
}

func (d CreateTableData) Table() string {
	return d.table
}

func (d CreateTableData) Schema() schema.Schema {
	return d.sche
}

func (d CreateTableData) Data() {}

func (p *Parser) CreateTable() (*CreateTableData, error) {
	d := &CreateTableData{}
	tok := p.lexer.NextToken()
	if tok.typ != create {
		return nil, fmt.Errorf("expected CREATE, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != table {
		return nil, fmt.Errorf("expected TABLE, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.table = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != lparen {
		return nil, fmt.Errorf("expected (, got %s", tok.literal)
	}
	sche, tok, err := p.fieldDefs()
	if err != nil {
		return nil, fmt.Errorf("failed to parse field definitions: %w", err)
	}
	d.sche = sche
	if tok.typ != rparen {
		return nil, fmt.Errorf("expected ), got %s", tok.literal)
	}
	return d, nil
}

type CreateViewData struct {
	view  string
	query *QueryData
}

func (d CreateViewData) ViewName() string {
	return d.view
}

func (d CreateViewData) ViewDef() string {
	return d.query.String()
}

func (d CreateViewData) Data() {}

func (p *Parser) CreateView() (*CreateViewData, error) {
	d := &CreateViewData{}
	tok := p.lexer.NextToken()
	if tok.typ != create {
		return nil, fmt.Errorf("expected CREATE, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != view {
		return nil, fmt.Errorf("expected VIEW, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.view = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != as {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	qd, err := p.Query()
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}
	d.query = qd
	return d, nil
}

type CreateIndexData struct {
	index string
	table string
	field schema.FieldName
}

func (d CreateIndexData) Index() string {
	return d.index
}

func (d CreateIndexData) Table() string {
	return d.table
}

func (d CreateIndexData) Field() schema.FieldName {
	return d.field
}

func (d CreateIndexData) Data() {}

func (p *Parser) CreateIndex() (*CreateIndexData, error) {
	d := &CreateIndexData{}
	tok := p.lexer.NextToken()
	if tok.typ != create {
		return nil, fmt.Errorf("expected CREATE, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != index {
		return nil, fmt.Errorf("expected INDEX, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.index = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != on {
		return nil, fmt.Errorf("expected ON, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.table = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != lparen {
		return nil, fmt.Errorf("expected (, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	d.field = schema.FieldName(tok.literal)
	tok = p.lexer.NextToken()
	if tok.typ != rparen {
		return nil, fmt.Errorf("expected ), got %s", tok.literal)
	}
	return d, nil
}

func (p *Parser) ToData() (Data, error) {
	tok := p.lexer.NextToken()
	switch tok.typ {
	case begin:
		return &BeginData{}, nil
	case rollback:
		return &RollbackData{}, nil
	case commit:
		return &CommitData{}, nil
	case selectTok:
		p.lexer.Reset()
		return p.Query()
	case insert:
		p.lexer.Reset()
		return p.Insert()
	case deleteTok:
		p.lexer.Reset()
		return p.Delete()
	case update:
		p.lexer.Reset()
		return p.Modify()
	case create:
		tok = p.lexer.NextToken()
		switch tok.typ {
		case table:
			p.lexer.Reset()
			return p.CreateTable()
		case view:
			p.lexer.Reset()
			return p.CreateView()
		case index:
			p.lexer.Reset()
			return p.CreateIndex()
		}
	}
	return nil, fmt.Errorf("unexpected token %s", tok.literal)
}

type BeginData struct{}

func (BeginData) Data() {}

type CommitData struct{}

func (CommitData) Data() {}

type RollbackData struct{}

func (RollbackData) Data() {}
