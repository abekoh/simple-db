package parse

import "errors"

type Lexer struct {
	s      string
	cursor int
}

func NewLexer(s string) *Lexer {
	return &Lexer{s: s}
}

func (l *Lexer) MatchDelim(c uint8) bool {
	if l.cursor >= len(l.s) {
		return false
	}
	return l.s[l.cursor] == c
}

func (l *Lexer) EatDelim(c uint8) error {
	if !l.MatchDelim(c) {
		return errors.New("bad syntax")
	}
	l.nextToken()
	return nil
}

func (l *Lexer) nextToken() {
	l.cursor++
	for l.cursor < len(l.s) && l.s[l.cursor] == ' ' {
		l.cursor++
	}
}
