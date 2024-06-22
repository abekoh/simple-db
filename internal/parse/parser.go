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

func (p *Parser) predicate() (query.Predicate, error) {
	terms := make([]query.Term, 0, 1)
	for {
		term, err := p.term()
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
		tok := p.lexer.NextToken()
		if tok.typ != and {
			break
		}
	}
	return query.NewPredicate(terms...), nil
}

func (p *Parser) term() (query.Term, error) {
	lhs, err := p.expression()
	if err != nil {
		return query.Term{}, err
	}
	tok := p.lexer.NextToken()
	if tok.typ != equal {
		return query.Term{}, fmt.Errorf("expected =, got %s", tok.literal)
	}
	rhs, err := p.expression()
	if err != nil {
		return query.Term{}, err
	}
	return query.NewTerm(lhs, rhs), nil
}

func (p *Parser) expression() (query.Expression, error) {
	tok := p.lexer.NextToken()
	switch tok.typ {
	case identifier:
		return schema.FieldName(tok.literal), nil
	case number:
		i, err := strconv.Atoi(tok.literal)
		if err != nil {
			return nil, err
		}
		return schema.ConstantInt32(i), nil
	case stringTok:
		return schema.ConstantStr(tok.literal), nil
	}
	return nil, fmt.Errorf("unexpected token %s", tok.literal)
}

type QueryData struct {
	fields []string
	tables []string
	pred   query.Predicate
}

func (p *Parser) Query() (*QueryData, error) {
	tok := p.lexer.NextToken()
	if tok.typ != selectTok {
		return nil, fmt.Errorf("expected SELECT, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	q := &QueryData{
		fields: make([]string, 0, 1),
		tables: make([]string, 0, 1),
	}
	q.fields = append(q.fields, tok.literal)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		q.fields = append(q.fields, tok.literal)
	}
	if tok.typ != from {
		return nil, fmt.Errorf("expected FROM, got %s", tok.literal)
	}
	tok = p.lexer.NextToken()
	if tok.typ != identifier {
		return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
	}
	q.tables = append(q.tables, tok.literal)
	for {
		tok = p.lexer.NextToken()
		if tok.typ != comma {
			break
		}
		tok = p.lexer.NextToken()
		if tok.typ != identifier {
			return nil, fmt.Errorf("expected identifier, got %s", tok.literal)
		}
		q.tables = append(q.tables, tok.literal)
	}
	if tok.typ == where {
		pred, err := p.predicate()
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
