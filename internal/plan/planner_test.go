package plan

import (
	"context"
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestBasicQueryPlanner(t *testing.T) {
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

func TestBasicUpdatePlanner_ExecuteCreateView(t *testing.T) {
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

	planner := NewPlanner(nil, NewBasicUpdatePlanner(db.MetadataMgr()))
	_, err = planner.ExecuteUpdate(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.ExecuteUpdate(`CREATE VIEW myview AS SELECT a FROM mytable`, tx)
	if err != nil {
		t.Fatal(err)
	}

	viewDef, ok, err := db.MetadataMgr().ViewDef("myview", tx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("view not found")
	}
	if viewDef != "SELECT a FROM mytable" {
		t.Errorf("unexpected view def: %s", viewDef)
	}
}

func TestBasicUpdatePlanner_ExecuteCreateIndex(t *testing.T) {
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

	planner := NewPlanner(nil, NewBasicUpdatePlanner(db.MetadataMgr()))
	_, err = planner.ExecuteUpdate(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.ExecuteUpdate(`CREATE INDEX myindex ON mytable (a)`, tx)
	if err != nil {
		t.Fatal(err)
	}

	indexInfo, err := db.MetadataMgr().IndexInfo("mytable", tx)
	if err != nil {
		t.Fatal(err)
	}
	aIndex, ok := indexInfo["a"]
	if !ok {
		t.Fatal("index for a not found")
	}
	if aIndex.IndexName() != "myindex" {
		t.Errorf("unexpected index name: %s", aIndex.IndexName())
	}
	if aIndex.FieldName() != "a" {
		t.Errorf("unexpected field name: %s", aIndex.FieldName())
	}
}

func TestBasicUpdatePlanner_ExecuteInsert(t *testing.T) {
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

	plan, err := planner.CreateQueryPlan(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := plan.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer scan.Close()
	ok, err := scan.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no rows")
	}
	aVal, err := scan.Int32("a")
	if err != nil {
		t.Fatal(err)
	}
	if aVal != 1 {
		t.Errorf("unexpected value: %d", aVal)
	}
	bVal, err := scan.Str("b")
	if err != nil {
		t.Fatal(err)
	}
	if bVal != "foo" {
		t.Errorf("unexpected value: %s", bVal)
	}
}

func TestBasicUpdatePlanner_ExecuteUpdate(t *testing.T) {
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
	c, err = planner.ExecuteUpdate(`UPDATE mytable SET b = 'bar' WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	plan, err := planner.CreateQueryPlan(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := plan.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer scan.Close()
	ok, err := scan.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no rows")
	}
	aVal, err := scan.Int32("a")
	if err != nil {
		t.Fatal(err)
	}
	if aVal != 1 {
		t.Errorf("unexpected value: %d", aVal)
	}
	bVal, err := scan.Str("b")
	if err != nil {
		t.Fatal(err)
	}
	if bVal != "bar" {
		t.Errorf("unexpected value: %s", bVal)
	}
}

func TestBasicUpdatePlanner_ExecuteDelete(t *testing.T) {
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
	c, err = planner.ExecuteUpdate(`DELETE FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	plan, err := planner.CreateQueryPlan(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := plan.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer scan.Close()
	ok, err := scan.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("unexpected rows")
	}
}
