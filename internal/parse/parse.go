package parse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
)

type tokenType string

const (
	identifier tokenType = "IDENTIFIER"

	selectTok tokenType = "SELECT"
	from      tokenType = "FROM"
	and       tokenType = "AND"
	where     tokenType = "WHERE"
	insert    tokenType = "INSERT"
	into      tokenType = "INTO"
	values    tokenType = "VALUES"
	deleteTok tokenType = "DELETE"
	update    tokenType = "UPDATE"
	set       tokenType = "SET"
	create    tokenType = "CREATE"
	table     tokenType = "TABLE"
	intTok    tokenType = "INT"
	varchar   tokenType = "VARCHAR"
	view      tokenType = "VIEW"
	as        tokenType = "AS"
	index     tokenType = "INDEX"
	on        tokenType = "ON"

	number    tokenType = "NUMBER"
	stringTok tokenType = "STRING"

	equal  tokenType = "EQUAL"
	comma  tokenType = "COMMA"
	lparen tokenType = "LPAREN"
	rparen tokenType = "RPAREN"

	illegal tokenType = "ILLEGAL"

	eof tokenType = "EOF"
)

var keywords = map[tokenType]struct{}{
	selectTok: {},
	from:      {},
	and:       {},
	where:     {},
	insert:    {},
	into:      {},
	values:    {},
	deleteTok: {},
	update:    {},
	set:       {},
	create:    {},
	table:     {},
	intTok:    {},
	varchar:   {},
	view:      {},
	as:        {},
	index:     {},
	on:        {},
}

func lookupToken(ident string) tokenType {
	ident = strings.ToUpper(ident)
	if _, ok := keywords[tokenType(ident)]; ok {
		return tokenType(ident)
	}
	return identifier
}

type token struct {
	typ     tokenType
	literal string
}

type Lexer struct {
	s          string
	cursor     int
	readCursor int
	char       byte
}

func NewLexer(s string) *Lexer {
	l := &Lexer{s: s}
	l.readChar()
	return l
}

func (l *Lexer) NextToken() token {
	for l.char == ' ' || l.char == '\t' || l.char == '\n' || l.char == '\r' {
		l.readChar()
	}

	switch l.char {
	case '=':
		l.readChar()
		return token{typ: equal, literal: "="}
	case ',':
		l.readChar()
		return token{typ: comma, literal: ","}
	case '(':
		l.readChar()
		return token{typ: lparen, literal: "("}
	case ')':
		l.readChar()
		return token{typ: rparen, literal: ")"}
	case '\'':
		tok := token{typ: stringTok, literal: l.readString()}
		l.readChar()
		return tok
	case 0:
		return token{typ: eof, literal: ""}
	default:
		if isLetter(l.char) {
			ident := l.readIdentifier()
			typ := lookupToken(ident)
			return token{typ: typ, literal: ident}
		} else if isDigit(l.char) {
			return token{typ: number, literal: l.readNumber()}
		} else {
			return token{typ: illegal, literal: string(l.char)}
		}
	}
}

func (l *Lexer) TokenIterator() func(func(token) bool) {
	return func(yield func(token) bool) {
		for {
			tok := l.NextToken()
			if !yield(tok) {
				break
			}
			if tok.typ == eof {
				break
			}
		}
	}
}

func (l *Lexer) readChar() {
	if l.readCursor >= len(l.s) {
		l.char = 0
	} else {
		l.char = l.s[l.readCursor]
	}
	l.cursor = l.readCursor
	l.readCursor++
}

func (l *Lexer) readIdentifier() string {
	start := l.cursor
	for isLetter(l.char) {
		l.readChar()
	}
	return l.s[start:l.cursor]
}

func (l *Lexer) readString() string {
	start := l.cursor + 1
	for {
		l.readChar()
		if l.char == '\'' || l.char == 0 {
			break
		}
	}
	return l.s[start:l.cursor]
}

func (l *Lexer) readNumber() string {
	start := l.cursor
	for isDigit(l.char) {
		l.readChar()
	}
	return l.s[start:l.cursor]
}

func isLetter(char byte) bool {
	return 'a' <= char && char <= 'z' || 'A' <= char && char <= 'Z' || char == '_'
}

func isDigit(char byte) bool {
	return '0' <= char && char <= '9'
}

type Parser struct {
	lexer *Lexer
}

func NewParser(s string) *Parser {
	return &Parser{lexer: NewLexer(s)}
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
