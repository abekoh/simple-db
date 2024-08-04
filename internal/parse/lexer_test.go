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
				{typ: number, literal: "1"},
				{typ: and, literal: "AND"},
				{typ: identifier, literal: "b"},
				{typ: equal, literal: "="},
				{typ: stringTok, literal: "foo"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `select a from mytable`,
			want: []token{
				{typ: selectTok, literal: "select"},
				{typ: identifier, literal: "a"},
				{typ: from, literal: "from"},
				{typ: identifier, literal: "mytable"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `SELECT a FROM mytable ORDER BY a ASC`,
			want: []token{
				{typ: selectTok, literal: "SELECT"},
				{typ: identifier, literal: "a"},
				{typ: from, literal: "FROM"},
				{typ: identifier, literal: "mytable"},
				{typ: order, literal: "ORDER"},
				{typ: by, literal: "BY"},
				{typ: identifier, literal: "a"},
				{typ: asc, literal: "ASC"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `SELECT a FROM mytable ORDER BY a DESC`,
			want: []token{
				{typ: selectTok, literal: "SELECT"},
				{typ: identifier, literal: "a"},
				{typ: from, literal: "FROM"},
				{typ: identifier, literal: "mytable"},
				{typ: order, literal: "ORDER"},
				{typ: by, literal: "BY"},
				{typ: identifier, literal: "a"},
				{typ: desc, literal: "DESC"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `SELECT a, b FROM mytable1 JOIN mytable2 ON mytable1.a = mytable2.b`,
			want: []token{
				{typ: selectTok, literal: "SELECT"},
				{typ: identifier, literal: "a"},
				{typ: comma, literal: ","},
				{typ: identifier, literal: "b"},
				{typ: from, literal: "FROM"},
				{typ: identifier, literal: "mytable1"},
				{typ: join, literal: "JOIN"},
				{typ: identifier, literal: "mytable2"},
				{typ: on, literal: "ON"},
				{typ: identifier, literal: "mytable1.a"},
				{typ: equal, literal: "="},
				{typ: identifier, literal: "mytable2.b"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `INSERT INTO mytable (a, b) VALUES (1, 'foo', $1)`,
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
				{typ: number, literal: "1"},
				{typ: comma, literal: ","},
				{typ: stringTok, literal: "foo"},
				{typ: comma, literal: ","},
				{typ: placeholder, literal: "$1"},
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
				{typ: number, literal: "1"},
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
				{typ: number, literal: "1"},
				{typ: where, literal: "WHERE"},
				{typ: identifier, literal: "b"},
				{typ: equal, literal: "="},
				{typ: stringTok, literal: "foo"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `CREATE TABLE mytable (a INT, b VARCHAR)`,
			want: []token{
				{typ: create, literal: "CREATE"},
				{typ: table, literal: "TABLE"},
				{typ: identifier, literal: "mytable"},
				{typ: lparen, literal: "("},
				{typ: identifier, literal: "a"},
				{typ: intTok, literal: "INT"},
				{typ: comma, literal: ","},
				{typ: identifier, literal: "b"},
				{typ: varchar, literal: "VARCHAR"},
				{typ: rparen, literal: ")"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `BEGIN`,
			want: []token{
				{typ: begin, literal: "BEGIN"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `COMMIT`,
			want: []token{
				{typ: commit, literal: "COMMIT"},
				{typ: eof, literal: ""},
			},
		},
		{
			input: `ROLLBACK`,
			want: []token{
				{typ: rollback, literal: "ROLLBACK"},
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
