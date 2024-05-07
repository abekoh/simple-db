package parse

import (
	"reflect"
	"testing"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input string
		want  []token
	}{
		{
			input: `SELECT a, b FROM mytable WHERE a = 1 AND b = 'foo'`,
			want: []token{
				{typ: selectTok, literal: "SELECT"},
				{typ: identifier, literal: "a"},
				{typ: comma, literal: ","},
				{typ: identifier, literal: "b"},
				{typ: from, literal: "FROM"},
				{typ: identifier, literal: "mytable"},
				{typ: where, literal: "WHERE"},
				{typ: identifier, literal: "a"},
				{typ: equal, literal: "="},
				{typ: intTok, literal: "1"},
				{typ: and, literal: "AND"},
				{typ: identifier, literal: "b"},
				{typ: equal, literal: "="},
				{typ: varchar, literal: "foo"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `INSERT INTO mytable (a, b) VALUES (1, 'foo')`,
			want: []token{
				{typ: insert, literal: "INSERT"},
				{typ: into, literal: "INTO"},
				{typ: identifier, literal: "mytable"},
				{typ: lparen, literal: "("},
				{typ: identifier, literal: "a"},
				{typ: comma, literal: ","},
				{typ: identifier, literal: "b"},
				{typ: rparen, literal: ")"},
				{typ: values, literal: "VALUES"},
				{typ: lparen, literal: "("},
				{typ: intTok, literal: "1"},
				{typ: comma, literal: ","},
				{typ: varchar, literal: "foo"},
				{typ: rparen, literal: ")"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `DELETE FROM mytable WHERE a = 1`,
			want: []token{
				{typ: deleteTok, literal: "DELETE"},
				{typ: from, literal: "FROM"},
				{typ: identifier, literal: "mytable"},
				{typ: where, literal: "WHERE"},
				{typ: identifier, literal: "a"},
				{typ: equal, literal: "="},
				{typ: intTok, literal: "1"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `UPDATE mytable SET a = 1 WHERE b = 'foo'`,
			want: []token{
				{typ: update, literal: "UPDATE"},
				{typ: identifier, literal: "mytable"},
				{typ: set, literal: "SET"},
				{typ: identifier, literal: "a"},
				{typ: equal, literal: "="},
				{typ: intTok, literal: "1"},
				{typ: where, literal: "WHERE"},
				{typ: identifier, literal: "b"},
				{typ: equal, literal: "="},
				{typ: varchar, literal: "foo"},
				{typ: eof, literal: ""},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := NewLexer(tt.input)
			var got []token
			for tok := range l.TokenIterator() {
				got = append(got, tok)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
