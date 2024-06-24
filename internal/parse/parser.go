package parse

import (
	"fmt"
	"strconv"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
)

type Parser struct {
	lexer *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{lexer: NewLexer(s)}
}

func (p *Parser) selectList() ([]schema.FieldName, token, error) {
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

type Command interface {
	Command()
}

type InsertData struct {
	table  string
	fields []schema.FieldName
	values []schema.Constant
}

func (d InsertData) Command() {}

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

type QueryData struct {
	fields []schema.FieldName
	tables []string
	pred   query.Predicate
}

func (d *QueryData) Fields() []schema.FieldName {
	return d.fields
}

func (d *QueryData) Tables() []string {
	return d.tables
}

func (d *QueryData) Predicate() query.Predicate {
	return d.pred
}

func (p *Parser) Query() (*QueryData, error) {
	q := &QueryData{}
	tok := p.lexer.NextToken()
	if tok.typ != selectTok {
		return nil, fmt.Errorf("expected SELECT, got %s", tok.literal)
	}
	selectList, tok, err := p.selectList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse select list: %w", err)
	}
	q.fields = selectList
	if tok.typ != from {
		return nil, fmt.Errorf("expected FROM, got %s", tok.literal)
	}
	tableList, tok, err := p.tableList()
	if err != nil {
		return nil, fmt.Errorf("failed to parse table list: %w", err)
	}
	q.tables = tableList
	if tok.typ == where {
		pred, _, err := p.predicate()
		if err != nil {
			return nil, fmt.Errorf("failed to parse predicate: %w", err)
		}
		q.pred = pred
	}
	return q, nil
}

type ModifyData struct {
	table string
	field string
	value query.Expression
	pred  query.Predicate
}

func (d ModifyData) Command() {}

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
	d.field = tok.literal
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

type DeleteData struct {
	table string
	pred  query.Predicate
}

func (d DeleteData) Table() string {
	return d.table
}

func (d DeleteData) Command() {}

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

type CreateTableData struct {
	table string
	sche  schema.Schema
}

func (d CreateTableData) Command() {}

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

func (d CreateViewData) Command() {}

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
	field string
}

func (d CreateIndexData) Command() {}

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
	d.field = tok.literal
	tok = p.lexer.NextToken()
	if tok.typ != rparen {
		return nil, fmt.Errorf("expected ), got %s", tok.literal)
	}
	return d, nil
}

func (p *Parser) UpdateCommand() (Command, error) {
	tok := p.lexer.NextToken()
	switch tok.typ {
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
