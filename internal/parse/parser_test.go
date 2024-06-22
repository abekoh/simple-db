package parse

import (
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record/schema"
)

func TestParser_Query(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *QueryData
		wantErr bool
	}{
		{
			name: "SELECT full",
			s:    "SELECT a, b FROM mytable WHERE a = 1 AND b = 'foo'",
			want: &QueryData{
				fields: []string{"a", "b"},
				tables: []string{"mytable"},
				pred: query.Predicate{
					query.NewTerm(schema.FieldName("a"), schema.ConstantInt32(1)),
					query.NewTerm(schema.FieldName("b"), schema.ConstantStr("foo")),
				},
			},
			wantErr: false,
		},
		{
			name: "SELECT without where",
			s:    "SELECT a, b FROM mytable",
			want: &QueryData{
				fields: []string{"a", "b"},
				tables: []string{"mytable"},
			},
			wantErr: false,
		},
		{
			name: "SELECT only one field",
			s:    "SELECT a FROM mytable",
			want: &QueryData{
				fields: []string{"a"},
				tables: []string{"mytable"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.Query()
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query() got = %v, want %v", got, tt.want)
			}
		})
	}
}
