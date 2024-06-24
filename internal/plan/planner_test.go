package plan

import (
	"context"
	"testing"

	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/server"
)

func TestBasicQueryPlanner(t *testing.T) {
	ctx := context.Background()
	db, err := server.NewSimpleDB(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sche1 := schema.NewSchema()
	sche1.AddInt32Field("a")
	sche1.AddStrField("b", 9)
	if err := db.MetadataMgr().CreateTable("mytable", sche1, tx); err != nil {
		t.Fatal(err)
	}

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), nil)
	plan, err := planner.CreateQueryPlan(`SELECT a, b FROM mytable WHERE mytable = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_ = plan
}
