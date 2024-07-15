package index_test

import (
	"context"
	"testing"

	"github.com/abekoh/simple-db/internal/index"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func TestNewBTreeIndex_OneIndex(t *testing.T) {
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
	field := schema.NewField(schema.Varchar, 5)
	sche.AddField("A", field)
	recordLayout := record.NewLayoutSchema(sche)
	ts, err := record.NewTableScan(tx, "mytable", recordLayout)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Insert(); err != nil {
		t.Fatal(err)
	}
	if err := ts.SetVal("A", schema.ConstantStr("aaa")); err != nil {
		t.Fatal(err)
	}

	idxLayout := index.NewIndexLayout(field)
	idx, err := index.NewBTreeIndex(tx, "myindex", idxLayout)
	if err != nil {
		t.Fatal(err)
	}

	if err := idx.Insert(schema.ConstantStr("aaa"), ts.RID()); err != nil {
		t.Fatal(err)
	}

	if err := idx.BeforeFirst(schema.ConstantStr("aaa")); err != nil {
		t.Fatal(err)
	}

	ok, err := idx.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no record found")
	}

	gotRID, err := idx.DataRID()
	if err != nil {
		t.Fatal(err)
	}
	if !gotRID.Equals(ts.RID()) {
		t.Fatalf("got %v, want %v", gotRID, ts.RID())
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}
