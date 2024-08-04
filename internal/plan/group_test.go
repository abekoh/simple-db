package plan_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestGroupByPlan(t *testing.T) {
	transaction.CleanupLockTable(t)
	ctx := context.Background()
	db, err := simpledb.New(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sche := schema.NewSchema()
	sche.AddStrField("department", 10)
	sche.AddInt32Field("score")
	if err := db.MetadataMgr().CreateTable("mytable", sche, tx); err != nil {
		t.Fatal(err)
	}

	layout := record.NewLayoutSchema(sche)
	updateScan, err := record.NewTableScan(tx, "mytable", layout)
	if err != nil {
		t.Fatal(err)
	}

	if err := updateScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		department string
		score      int32
	}{
		{"math", 93},
		{"math", 87},
		{"math", 92},
		{"math", 85},
		{"english", 85},
		{"english", 90},
		{"english", 88},
	} {
		if err := updateScan.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := updateScan.SetVal("department", schema.ConstantStr(v.department)); err != nil {
			t.Fatal(err)
		}
		if err := updateScan.SetVal("score", schema.ConstantInt32(v.score)); err != nil {
			t.Fatal(err)
		}
	}
	if err := updateScan.Close(); err != nil {
		t.Fatal(err)
	}

	tablePlan, err := plan.NewTablePlan("mytable", tx, db.MetadataMgr())
	if err != nil {
		t.Fatal(err)
	}
	groupByPlan := plan.NewGroupByPlan(tx,
		tablePlan,
		[]schema.FieldName{"department"},
		[]plan.AggregationFunc{
			plan.NewCountFunc("count_score"),
			plan.NewMaxFunc("score", "max_score"),
			plan.NewMinFunc("score", "min_score"),
			plan.NewSumFunc("score", "sum_score"),
		},
	)
	sortPlan := plan.NewSortPlan(tx, groupByPlan, query.Order{query.OrderElement{Field: "department", OrderType: query.Asc}})
	projectPlan := plan.NewProjectPlan(sortPlan, []schema.FieldName{"department", "count_score", "max_score", "min_score"})

	queryScan, err := projectPlan.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := queryScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	res := make([]string, 0, 2)
	for {
		ok, err := queryScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		department, err := queryScan.Str("department")
		if err != nil {
			t.Fatal(err)
		}
		countScore, err := queryScan.Int32("count_score")
		if err != nil {
			t.Fatal(err)
		}
		maxScore, err := queryScan.Int32("max_score")
		if err != nil {
			t.Fatal(err)
		}
		minScore, err := queryScan.Int32("min_score")
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, fmt.Sprintf("%s: count=%d, max=%d, min=%d", department, countScore, maxScore, minScore))
	}
	if err := queryScan.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []string{
		"english: count=3, max=90, min=85",
		"math: count=4, max=93, min=85",
	}) {
		t.Fatalf("unexpected result: %v", res)
	}
}
