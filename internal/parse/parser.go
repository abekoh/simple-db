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

func (p *Parser) selectList() ([]string, token, error) {
	fields := make([]string, 0, 1)
	tok := p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	fields = append(fields, tok.literal)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, tok, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		fields = append(fields, tok.literal)
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

func (p *Parser) predicate() (query.Predicate, token, error) {
	terms := make([]query.Term, 0, 1)
	var tok token
	for {
		term, tok, err := p.term()
		if err != nil {
			return nil, tok, err
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
		return query.Term{}, tok, err
	}
	tok = p.lexer.NextToken()
	if tok.typ != equal {
		return query.Term{}, tok, fmt.Errorf("expected =, got %s", tok.literal)
	}
	rhs, tok, err := p.expression()
	if err != nil {
		return query.Term{}, tok, err
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
			return nil, tok, err
		}
		return schema.ConstantInt32(i), tok, nil
	case stringTok:
		return schema.ConstantStr(tok.literal), tok, nil
	}
	return nil, tok, fmt.Errorf("unexpected token %s", tok.literal)
}

type QueryData struct {
	fields []string
	tables []string
	pred   query.Predicate
}

func (p *Parser) Query() (*QueryData, error) {
	q := &QueryData{}
	tok := p.lexer.NextToken()
	if tok.typ != selectTok {
		return nil, fmt.Errorf("expected SELECT, got %s", tok.literal)
	}
	selectList, tok, err := p.selectList()
	if err != nil {
		return nil, err
	}
	q.fields = selectList
	if tok.typ != from {
		return nil, fmt.Errorf("expected FROM, got %s", tok.literal)
	}
	tableList, tok, err := p.tableList()
	if err != nil {
		return nil, err
	}
	q.tables = tableList
	if tok.typ == where {
		pred, _, err := p.predicate()
		if err != nil {
			return nil, err
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

type InsertData struct {
	table  string
	fields []string
	values []schema.Constant
}

type DeleteData struct {
	table string
	pred  query.Predicate
}

type CreateTableData struct {
	table string
	sche  schema.Schema
}

type CreateViewData struct {
	view  string
	query QueryData
}

type CreateIndexData struct {
	index string
	table string
	field string
}
