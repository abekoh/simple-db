package plan_test

import (
	"context"
	"reflect"
	"testing"

	. "github.com/abekoh/simple-db/internal/plan"
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), nil, db.MetadataMgr())
	plan, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	info := plan.(Plan).Info()
	if !reflect.DeepEqual(info, Info{
		NodeType: "Project",
		Conditions: map[string][]string{
			"fieldNames": {"a", "b"},
		},
		Children: []Info{
			{
				NodeType: "Select",
				Conditions: map[string][]string{
					"predicate": {"a=1"},
				},
				Children: []Info{
					{
						NodeType: "Table",
						Conditions: map[string][]string{
							"tableName": {"mytable"},
						},
					},
				},
			},
		},
	}) {
		t.Errorf("unexpected plan: %v", info)
	}
}

func TestBasicQueryPlanner_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), nil, db.MetadataMgr())
	prepared, err := planner.Prepare(`SELECT a, b FROM mytable WHERE a = $1 AND b = $2`, tx)
	if err != nil {
		t.Fatal(err)
	}
	info := prepared.(Plan).Info()
	if !reflect.DeepEqual(info, Info{
		NodeType:   "Project",
		Conditions: map[string][]string{"fieldNames": {"a", "b"}},
		Children: []Info{
			{
				NodeType:   "Select",
				Conditions: map[string][]string{"predicate": {"a=$1 AND b=$2"}},
				Children: []Info{
					{
						NodeType:   "Table",
						Conditions: map[string][]string{"tableName": {"mytable"}},
					},
				},
			},
		},
	}) {
		t.Errorf("unexpected prepared: %v", info)
	}

	b, err := planner.Bind(prepared, map[int]any{1: int32(1), 2: "foo"}, tx)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	info = p.(Plan).Info()
	if !reflect.DeepEqual(info, Info{
		NodeType:   "Project",
		Conditions: map[string][]string{"fieldNames": {"a", "b"}},
		Children: []Info{
			{
				NodeType:   "Select",
				Conditions: map[string][]string{"predicate": {"a=1 AND b=foo"}},
				Children: []Info{
					{
						NodeType:   "Table",
						Conditions: map[string][]string{"tableName": {"mytable"}},
					},
				},
			},
		},
	}) {
		t.Errorf("unexpected plan: %v", info)
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

	planner := NewPlanner(nil, NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
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

	planner := NewPlanner(nil, NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Execute(`CREATE VIEW myview AS SELECT a FROM mytable`, tx)
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

	planner := NewPlanner(nil, NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Execute(`CREATE INDEX myindex ON mytable (a)`, tx)
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestBasicUpdatePlanner_ExecuteInsert_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := planner.Prepare(`INSERT INTO mytable (a, b) VALUES ($1, $2)`, tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.Bind(prepared, map[int]any{1: int32(1), 2: "foo"}, tx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	c, err = planner.Execute(`UPDATE mytable SET b = 'bar' WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestBasicUpdatePlanner_ExecuteUpdate_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	prepared, err := planner.Prepare(`UPDATE mytable SET b = $1 WHERE a = $2`, tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.Bind(prepared, map[int]any{1: "bar", 2: int32(1)}, tx)
	if err != nil {
		t.Fatal(err)
	}
	c, err = planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	c, err = planner.Execute(`DELETE FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestBasicUpdatePlanner_ExecuteDelete_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewBasicUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	prepared, err := planner.Prepare(`DELETE FROM mytable WHERE a = $1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.Bind(prepared, map[int]any{1: int32(1)}, tx)
	if err != nil {
		t.Fatal(err)
	}
	c, err = planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestIndexUpdatePlanner_ExecuteCreateTable(t *testing.T) {
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

	planner := NewPlanner(nil, NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
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

func TestIndexUpdatePlanner_ExecuteCreateView(t *testing.T) {
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

	planner := NewPlanner(nil, NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Execute(`CREATE VIEW myview AS SELECT a FROM mytable`, tx)
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

func TestIndexUpdatePlanner_ExecuteCreateIndex(t *testing.T) {
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

	planner := NewPlanner(nil, NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Execute(`CREATE INDEX myindex ON mytable (a)`, tx)
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

func TestIndexUpdatePlanner_ExecuteInsert(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestIndexUpdatePlanner_ExecuteInsert_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	prepared, err := planner.Prepare(`INSERT INTO mytable (a, b) VALUES ($1, $2)`, tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.Bind(prepared, map[int]any{1: int32(1), 2: "foo"}, tx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestIndexUpdatePlanner_ExecuteUpdate(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	c, err = planner.Execute(`UPDATE mytable SET b = 'bar' WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestIndexUpdatePlanner_ExecuteUpdate_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	prepared, err := planner.Prepare(`UPDATE mytable SET b = $1 WHERE a = $2`, tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.Bind(prepared, map[int]any{1: "bar", 2: int32(1)}, tx)
	if err != nil {
		t.Fatal(err)
	}
	c, err = planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestIndexUpdatePlanner_ExecuteDelete(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	c, err = planner.Execute(`DELETE FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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

func TestIndexUpdatePlanner_ExecuteDelete_Prepared(t *testing.T) {
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

	planner := NewPlanner(NewBasicQueryPlanner(db.MetadataMgr()), NewIndexUpdatePlanner(db.MetadataMgr()), db.MetadataMgr())
	_, err = planner.Execute(`CREATE TABLE mytable (a INT, b VARCHAR(9))`, tx)
	if err != nil {
		t.Fatal(err)
	}

	c, err := planner.Execute(`INSERT INTO mytable (a, b) VALUES (1, 'foo')`, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}
	prepared, err := planner.Prepare(`DELETE FROM mytable WHERE a = $1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := planner.Bind(prepared, map[int]any{1: int32(1)}, tx)
	if err != nil {
		t.Fatal(err)
	}
	c, err = planner.ExecuteBound(b, tx)
	if err != nil {
		t.Fatal(err)
	}
	if c.(CommandResult).Count != 1 {
		t.Errorf("unexpected count: %d", c)
	}

	p, err := planner.Execute(`SELECT a, b FROM mytable WHERE a = 1`, tx)
	if err != nil {
		t.Fatal(err)
	}
	plan := p.(Plan)
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
