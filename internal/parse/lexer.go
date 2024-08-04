package parse

import (
	"fmt"
	"strings"
)

type tokenType string

const (
	identifier tokenType = "IDENTIFIER"

	selectTok   tokenType = "SELECT"
	from        tokenType = "FROM"
	and         tokenType = "AND"
	where       tokenType = "WHERE"
	insert      tokenType = "INSERT"
	into        tokenType = "INTO"
	values      tokenType = "VALUES"
	deleteTok   tokenType = "DELETE"
	update      tokenType = "UPDATE"
	set         tokenType = "SET"
	create      tokenType = "CREATE"
	table       tokenType = "TABLE"
	intTok      tokenType = "INT"
	varchar     tokenType = "VARCHAR"
	view        tokenType = "VIEW"
	as          tokenType = "AS"
	index       tokenType = "INDEX"
	join        tokenType = "JOIN"
	on          tokenType = "ON"
	placeholder tokenType = "PLACEHOLDER"
	begin       tokenType = "BEGIN"
	commit      tokenType = "COMMIT"
	rollback    tokenType = "ROLLBACK"
	order       tokenType = "ORDER"
	group       tokenType = "GROUP"
	by          tokenType = "BY"
	asc         tokenType = "ASC"
	desc        tokenType = "DESC"

	number    tokenType = "NUMBER"
	stringTok tokenType = "STRING"

	equal  tokenType = "EQUAL"
	comma  tokenType = "COMMA"
	lparen tokenType = "LPAREN"
	rparen tokenType = "RPAREN"

	illegal tokenType = "ILLEGAL"

	max   tokenType = "MAX"
	min   tokenType = "MIN"
	sum   tokenType = "SUM"
	count tokenType = "COUNT"

	asterisk tokenType = "*"

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
	join:      {},
	on:        {},
	begin:     {},
	commit:    {},
	rollback:  {},
	order:     {},
	group:     {},
	by:        {},
	asc:       {},
	desc:      {},
	max:       {},
	min:       {},
	sum:       {},
	count:     {},
	asterisk:  {},
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
	case '*':
		l.readChar()
		return token{typ: asterisk, literal: "*"}
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
	case '$':
		l.readChar()
		return token{typ: placeholder, literal: fmt.Sprintf("$%s", l.readNumber())}
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
	for isLetter(l.char) || isDigit(l.char) || l.char == '.' {
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

func (l *Lexer) Reset() {
	l.cursor = 0
	l.readCursor = 0
	var char byte
	l.char = char
	l.readChar()
}
