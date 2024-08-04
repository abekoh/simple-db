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
		planInfo plan.Info
	}
	for _, tt := range []test{
		{
			name:     "one table, no index",
			snapshot: "tables_data",
			query:    "SELECT name FROM students WHERE student_id = 200588",
			planInfo: plan.Info{
				NodeType:      "Project",
				Conditions:    map[string][]string{"fields": {"name"}},
				BlockAccessed: 770,
				RecordsOutput: 2,
				Children: []plan.Info{
					{
						NodeType:      "Select",
						Conditions:    map[string][]string{"predicate": {"student_id=200588"}},
						BlockAccessed: 770,
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
		{
			name:     "one table, use index",
			snapshot: "tables_indexes_data",
			query:    "SELECT name FROM students WHERE student_id = 200588",
			planInfo: plan.Info{
				NodeType:      "Project",
				Conditions:    map[string][]string{"fields": {"name"}},
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
			},
		},
		{
			name:     "join two tables, no index",
			snapshot: "tables_data",
			query:    "SELECT name, department_name FROM students JOIN departments ON major_id = department_id WHERE student_id = 200588",
			planInfo: plan.Info{
				NodeType:      "Project",
				Conditions:    map[string][]string{"fields": {"name", "department_name"}},
				BlockAccessed: 776,
				RecordsOutput: 0,
				Children: []plan.Info{
					{
						NodeType:      "Select",
						Conditions:    map[string][]string{"predicate": {"major_id=department_id"}},
						BlockAccessed: 776,
						RecordsOutput: 0,
						Children: []plan.Info{
							{
								NodeType:      "MultiBufferProduct",
								BlockAccessed: 776,
								RecordsOutput: 200,
								Children: []plan.Info{
									{
										NodeType:      "Table",
										Conditions:    map[string][]string{"table": {"departments"}},
										BlockAccessed: 6,
										RecordsOutput: 100,
									},
									{
										NodeType:      "Select",
										Conditions:    map[string][]string{"predicate": {"student_id=200588"}},
										BlockAccessed: 770,
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
					},
				},
			},
		},
		{
			name:     "join two tables, use index",
			snapshot: "tables_indexes_data",
			query:    "SELECT name, department_name FROM students JOIN departments ON major_id = department_id WHERE student_id = 200588",
			planInfo: plan.Info{
				NodeType:      "Project",
				Conditions:    map[string][]string{"fields": {"name", "department_name"}},
				BlockAccessed: 8,
				RecordsOutput: 0,
				Children: []plan.Info{
					{
						NodeType:      "Select",
						Conditions:    map[string][]string{"predicate": {"major_id=department_id"}},
						BlockAccessed: 8,
						RecordsOutput: 0,
						Children: []plan.Info{
							{
								NodeType:      "IndexJoin",
								Conditions:    map[string][]string{"index": {"departments_pkey"}, "field": {"major_id"}},
								BlockAccessed: 8,
								RecordsOutput: 4,
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
									{
										NodeType:      "Table",
										Conditions:    map[string][]string{"table": {"departments"}},
										BlockAccessed: 6,
										RecordsOutput: 100,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name:     "sort",
			snapshot: "tables_data",
			query:    "SELECT name FROM students ORDER BY name",
			planInfo: plan.Info{
				NodeType:      "Project",
				Conditions:    map[string][]string{"fields": {"name"}},
				BlockAccessed: 750,
				RecordsOutput: 10000,
				Children: []plan.Info{
					{
						NodeType:      "Sort",
						Conditions:    map[string][]string{"sortFields": {"name"}},
						BlockAccessed: 750,
						RecordsOutput: 10000,
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
			if diff := cmp.Diff(tt.planInfo, p.Info()); diff != "" {
				t.Errorf("(-got, +expected)\n%s", diff)
			}
		})
	}
}
