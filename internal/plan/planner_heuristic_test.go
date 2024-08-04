package plan_test

import (
	"context"
	"testing"

	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/testdata"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestHeuristicQueryPlanner(t *testing.T) {
	transaction.CleanupLockTable(t)
	ctx := context.Background()
	dir := t.TempDir()
	if err := testdata.CopySnapshotData("tables_indexes_data", dir); err != nil {
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
	res, err := db.Planner().Execute("SELECT table_name FROM table_catalog", tx)
	if err != nil {
		t.Fatal(err)
	}
	_ = res
}
