package plan_test

import (
	"context"
	"testing"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/testdata"
	"github.com/abekoh/simple-db/internal/transaction"
	"github.com/google/go-cmp/cmp"
)

func TestHeuristicQueryPlanner_QueryPlans(t *testing.T) {
	type test struct {
		name     string
		snapshot string
		query    string
		planStr  string
	}
	for _, tt := range []test{
		{
			name:     "one table, use index",
			snapshot: "tables_indexes_data",
			query:    "SELECT student_name FROM students WHERE student_id = 200588",
			planStr:  "Project{student_name}(Select{student_id=200588}(IndexSelect{student_id=200588(students_pkey)}(Table{students})))",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transaction.CleanupLockTable(t)
			ctx := context.Background()
			dir := t.TempDir()
			if err := testdata.CopySnapshotData(tt.snapshot, dir); err != nil {
				t.Fatal(err)
			}
			db, err := simpledb.New(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.NewTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			res, err := db.Planner().Execute(tt.query, tx)
			if err != nil {
				t.Fatal(err)
			}
			p, ok := res.(plan.Plan)
			if !ok {
				t.Fatalf("unexpected type %T", res)
			}
			got := p.Info()
			expected := plan.Info{
				NodeType:      "Project",
				Conditions:    map[string][]string{"fields": {"student_name"}},
				BlockAccessed: 2,
				RecordsOutput: 2,
				Children: []plan.Info{
					{
						NodeType:      "Select",
						Conditions:    map[string][]string{"predicate": {"student_id=200588"}},
						BlockAccessed: 2,
						RecordsOutput: 2,
						Children: []plan.Info{
							{
								NodeType:      "IndexSelect",
								Conditions:    map[string][]string{"index": {"students_pkey"}, "value": {"200588"}},
								BlockAccessed: 2,
								RecordsOutput: 2,
								Children: []plan.Info{
									{
										NodeType:      "Table",
										Conditions:    map[string][]string{"table": {"students"}},
										BlockAccessed: 770,
										RecordsOutput: 10000,
									},
								},
							},
						},
					},
				},
			}
			if diff := cmp.Diff(expected, got); diff != "" {
				t.Errorf("(-got, +expected)\n%s", diff)
			}
		})
	}
}
