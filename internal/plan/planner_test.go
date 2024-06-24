package plan

import (
	"context"
	"reflect"
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

	sche := schema.NewSchema()
	sche.AddInt32Field("a")
	sche.AddStrField("b", 9)
	if err := db.MetadataMgr().CreateTable("mytable", sche, tx); err != nil {
		t.Fatal(err)
	}

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), nil)
	plan, err := planner.CreateQueryPlan(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if plan.String() != "Project{a,b}(Select{a=1}(Table{mytable}))" {
		t.Errorf("unexpected plan: %s", plan.String())
	}
}

func TestBasicUpdatePlanner_ExecuteCreateTable(t *testing.T) {
	ctx := context.Background()
	db, err := server.NewSimpleDB(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	planner := NewPlanner(nil, NewBasicUpdatePlanner(db.MetadataMgr()))
	_, err = planner.ExecuteUpdate(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	l, err := db.MetadataMgr().Layout("mytable", tx)
	if err != nil {
		t.Fatal(err)
	}
	expectedSche := schema.NewSchema()
	expectedSche.AddInt32Field("a")
	expectedSche.AddStrField("b", 9)
	if !reflect.DeepEqual(expectedSche, *l.Schema()) {
		t.Errorf("unexpected schema: %v", l.Schema())
	}
}

func TestBasicUpdatePlanner_ExecuteInsert(t *testing.T) {
	ctx := context.Background()
	db, err := server.NewSimpleDB(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()))
	_, err = planner.ExecuteUpdate(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.ExecuteUpdate(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c != 1 {
		t.Errorf("unexpected count: %d", c)
	}
}
