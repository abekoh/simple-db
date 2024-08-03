package plan_test

import (
	"context"
	"testing"

	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestHeuristicQueryPlanner(t *testing.T) {
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
	_ = tx

	// TODO
}
