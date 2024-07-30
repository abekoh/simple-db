package materialize_test

import (
	"context"
	"testing"

	"github.com/abekoh/simple-db/internal/materialize"
	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func TestGroupByPlan(t *testing.T) {
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
	sche.AddInt32Field("A")
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
	for _, v := range []int32{1, 3, 12, 9, 7} {
		if err := updateScan.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := updateScan.SetVal("A", schema.ConstantInt32(v)); err != nil {
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
	groupByPlan := materialize.NewGroupByPlan(tx,
		tablePlan,
		[]schema.FieldName{"A"},
		[]materialize.AggregationFunc{materialize.NewCountFunc("countA")},
	)
	projectPlan := plan.NewProjectPlan(groupByPlan, []schema.FieldName{"countA"})

	queryScan, err := projectPlan.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := queryScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	var result int32
	for {
		ok, err := queryScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		val, err := queryScan.Int32("countA")
		if err != nil {
			t.Fatal(err)
		}
		result = val
	}
	if result != 5 {
		t.Fatalf("got %d, want 5", result)
	}
}
