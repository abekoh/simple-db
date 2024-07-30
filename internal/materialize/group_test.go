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
	sche.AddInt32Field("id")
	sche.AddInt32Field("value")
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
	for i, v := range []int32{4, 3, 12, 9, 7} {
		if err := updateScan.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := updateScan.SetInt32("id", int32(i)); err != nil {
			t.Fatal(err)
		}
		if err := updateScan.SetVal("value", schema.ConstantInt32(v)); err != nil {
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
		[]schema.FieldName{"id"},
		[]materialize.AggregationFunc{materialize.NewMaxFunc("value")},
	)
	projectPlan := plan.NewProjectPlan(groupByPlan, []schema.FieldName{"id", "value"})

	queryScan, err := projectPlan.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := queryScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	var resID, resValue int32
	for {
		ok, err := queryScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		id, err := queryScan.Int32("id")
		if err != nil {
			t.Fatal(err)
		}
		resID = id
		value, err := queryScan.Int32("value")
		if err != nil {
			t.Fatal(err)
		}
		resValue = value
	}
	if resID != 4 {
		t.Errorf("expected 4, but got %d", resID)
	}
	if resValue != 12 {
		t.Errorf("expected 12, but got %d", resValue)
	}
}
