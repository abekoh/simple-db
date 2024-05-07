package parse

import (
	"testing"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input string
		want  *Lexer
	}{
		{
			input: `SELECT a, b, c FROM table WHERE a = 1`,
			want:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			l := NewLexer(tt.input)
		})
	}
}
