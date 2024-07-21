package materialize_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/materialize"
	plan2 "github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func TestSortPlan(t *testing.T) {
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
	sche.AddStrField("B", 9)
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
	for _, v := range []string{"rec2", "rec5", "rec1", "rec4", "rec3"} {
		if err := updateScan.Insert(); err != nil {
			t.Fatal(err)
		}
		rec := schema.ConstantStr(v)
		if err := updateScan.SetVal("B", rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := updateScan.Close(); err != nil {
		t.Fatal(err)
	}

	tablePlan, err := plan2.NewTablePlan("mytable", tx, db.MetadataMgr())
	if err != nil {
		t.Fatal(err)
	}
	sortPlan := materialize.NewSortPlan(tx, tablePlan, []schema.FieldName{"B"})
	projectPlan := plan2.NewProjectPlan(sortPlan, []schema.FieldName{"B"})
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
		val, err := queryScan.Str("B")
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, val)
	}
	if err := queryScan.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []string{"rec1", "rec2", "rec3", "rec4", "rec5"}) {
		t.Fatalf("unexpected result: %v", res)
	}
}
