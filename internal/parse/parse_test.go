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
