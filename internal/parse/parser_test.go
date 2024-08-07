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
			s:    "SELECT a, b, c FROM mytable WHERE a = 1 AND b = 'foo' AND c = $1",
			want: &QueryData{
				fields: []schema.FieldName{"a", "b", "c"},
				tables: []string{"mytable"},
				pred: query.Predicate{
					query.NewTerm(schema.FieldName("a"), schema.ConstantInt32(1)),
					query.NewTerm(schema.FieldName("b"), schema.ConstantStr("foo")),
					query.NewTerm(schema.FieldName("c"), schema.Placeholder(1)),
				},
			},
			wantErr: false,
		},
		{
			name: "SELECT without where",
			s:    "SELECT a, b FROM mytable",
			want: &QueryData{
				fields: []schema.FieldName{"a", "b"},
				tables: []string{"mytable"},
			},
			wantErr: false,
		},
		{
			name: "SELECT only one field",
			s:    "SELECT a FROM mytable",
			want: &QueryData{
				fields: []schema.FieldName{"a"},
				tables: []string{"mytable"},
			},
			wantErr: false,
		},
		{
			name: "SELECT product",
			s:    "SELECT a, x FROM mytable1, mytable2",
			want: &QueryData{
				fields: []schema.FieldName{"a", "x"},
				tables: []string{"mytable1", "mytable2"},
			},
			wantErr: false,
		},
		{
			name: "SELECT with join",
			s:    "SELECT a, x FROM mytable1 JOIN mytable2 ON a = x",
			want: &QueryData{
				fields: []schema.FieldName{"a", "x"},
				tables: []string{"mytable1", "mytable2"},
				pred: query.Predicate{
					query.NewTerm(schema.FieldName("a"), schema.FieldName("x")),
				},
			},
			wantErr: false,
		},
		{
			name: "SELECT with 2 joins",
			s:    "SELECT a, x, y FROM mytable1 JOIN mytable2 ON a = x JOIN mytable3 ON x = y",
			want: &QueryData{
				fields: []schema.FieldName{"a", "x", "y"},
				tables: []string{"mytable1", "mytable2", "mytable3"},
				pred: query.Predicate{
					query.NewTerm(schema.FieldName("a"), schema.FieldName("x")),
					query.NewTerm(schema.FieldName("x"), schema.FieldName("y")),
				},
			},
		},
		{
			name: "SELECT with 2 joins and where",
			s:    "SELECT a, x, y FROM mytable1 JOIN mytable2 ON a = x JOIN mytable3 ON x = y WHERE a = 1",
			want: &QueryData{
				fields: []schema.FieldName{"a", "x", "y"},
				tables: []string{"mytable1", "mytable2", "mytable3"},
				pred: query.Predicate{
					query.NewTerm(schema.FieldName("a"), schema.FieldName("x")),
					query.NewTerm(schema.FieldName("x"), schema.FieldName("y")),
					query.NewTerm(schema.FieldName("a"), schema.ConstantInt32(1)),
				},
			},
		},
		{
			name: "SELECT with ORDER BY",
			s:    "SELECT a, b FROM mytable ORDER BY a",
			want: &QueryData{
				fields: []schema.FieldName{"a", "b"},
				tables: []string{"mytable"},
				order: query.Order{
					query.OrderElement{Field: "a", OrderType: query.Asc},
				},
			},
		},
		{
			name: "SELECT with ORDER BY DESC",
			s:    "SELECT a, b FROM mytable ORDER BY a DESC",
			want: &QueryData{
				fields: []schema.FieldName{"a", "b"},
				tables: []string{"mytable"},
				order: query.Order{
					query.OrderElement{Field: "a", OrderType: query.Desc},
				},
			},
		},
		{
			name: "SELECT with ORDER BY ASC",
			s:    "SELECT a, b FROM mytable ORDER BY a ASC",
			want: &QueryData{
				fields: []schema.FieldName{"a", "b"},
				tables: []string{"mytable"},
				order: query.Order{
					query.OrderElement{Field: "a", OrderType: query.Asc},
				},
			},
		},
		{
			name: "SELECT with GROUP BY",
			s:    "SELECT a, MAX(b) AS max_b FROM mytable GROUP BY a",
			want: &QueryData{
				fields: []schema.FieldName{"a", "max_b"},
				aggregationFuncs: []query.AggregationFunc{
					query.NewMaxFunc("b", "max_b"),
				},
				tables:      []string{"mytable"},
				groupFields: []schema.FieldName{"a"},
			},
		},
		{
			name: "SELECT with GROUP BY using *",
			s:    "SELECT a, COUNT(*) AS max_b FROM mytable GROUP BY a",
			want: &QueryData{
				fields: []schema.FieldName{"a", "max_b"},
				aggregationFuncs: []query.AggregationFunc{
					query.NewCountFunc("*", "max_b"),
				},
				tables:      []string{"mytable"},
				groupFields: []schema.FieldName{"a"},
			},
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

func TestParser_Insert(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *InsertData
		wantErr bool
	}{
		{
			name: "INSERT",
			s:    "INSERT INTO mytable (a, b, c) VALUES (1, 'foo', $1)",
			want: &InsertData{
				table:  "mytable",
				fields: []schema.FieldName{"a", "b", "c"},
				values: []schema.Constant{
					schema.ConstantInt32(1),
					schema.ConstantStr("foo"),
					schema.Placeholder(1),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.Insert()
			if (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Insert() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_Modify(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *ModifyData
		wantErr bool
	}{
		{
			name: "UPDATE full",
			s:    "UPDATE mytable SET a = 1 WHERE b = 'foo'",
			want: &ModifyData{
				table: "mytable",
				field: "a",
				value: schema.ConstantInt32(1),
				pred: query.Predicate{
					query.NewTerm(
						schema.FieldName("b"),
						schema.ConstantStr("foo")),
				},
			},
			wantErr: false,
		},
		{
			name: "UPDATE without where",
			s:    "UPDATE mytable SET a = 1",
			want: &ModifyData{
				table: "mytable",
				field: "a",
				value: schema.ConstantInt32(1),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.Modify()
			if (err != nil) != tt.wantErr {
				t.Errorf("Modify() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Modify() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_Delete(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *DeleteData
		wantErr bool
	}{
		{
			name: "DELETE full",
			s:    "DELETE FROM mytable WHERE a = 1",
			want: &DeleteData{
				table: "mytable",
				pred: query.Predicate{
					query.NewTerm(
						schema.FieldName("a"),
						schema.ConstantInt32(1),
					),
				},
			},
			wantErr: false,
		},
		{
			name: "DELETE without where",
			s:    "DELETE FROM mytable",
			want: &DeleteData{
				table: "mytable",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.Delete()
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_CreateTable(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *CreateTableData
		wantErr bool
	}{
		{
			name: "CREATE TABLE",
			s:    "CREATE TABLE mytable (a INT, b VARCHAR(10))",
			want: &CreateTableData{
				table: "mytable",
				sche: func() schema.Schema {
					s := schema.NewSchema()
					s.AddField("a", schema.NewInt32Field())
					s.AddField("b", schema.NewVarcharField(10))
					return s
				}(),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.CreateTable()
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateTable() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_CreateView(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *CreateViewData
		wantErr bool
	}{
		{
			name: "CREATE VIEW",
			s:    "CREATE VIEW myview AS SELECT a, b FROM mytable WHERE a = 1 AND b = 'foo'",
			want: &CreateViewData{
				view: "myview",
				query: &QueryData{
					fields: []schema.FieldName{"a", "b"},
					tables: []string{"mytable"},
					pred: query.Predicate{
						query.NewTerm(schema.FieldName("a"), schema.ConstantInt32(1)),
						query.NewTerm(schema.FieldName("b"), schema.ConstantStr("foo")),
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.CreateView()
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateView() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateView() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_CreateIndex(t *testing.T) {
	tests := []struct {
		name    string
		s       string
		want    *CreateIndexData
		wantErr bool
	}{
		{
			name: "CREATE INDEX",
			s:    "CREATE INDEX myindex ON mytable (a)",
			want: &CreateIndexData{
				index: "myindex",
				table: "mytable",
				field: "a",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(tt.s)
			got, err := p.CreateIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateIndex() got = %v, want %v", got, tt.want)
			}
		})
	}
}
