package materialize_test

import (
	"context"
	"fmt"
	"reflect"
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
		[]schema.FieldName{"id", "value"},
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

	res := make([]string, 0, 5)
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
		value, err := queryScan.Int32("value")
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, fmt.Sprintf("%d:%d", id, value))
	}
	if err := queryScan.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []string{"0:12", "1:9", "2:7", "3:4", "4:3"}) {
		t.Fatalf("got %v, want [0:12, 1:9, 2:7, 3:4, 4:3]", res)
	}
}
