package index_test

import (
	"context"
	"fmt"
	"strings"
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
	field := schema.NewField(schema.Varchar, 10)
	sche.AddField("A", field)
	recordLayout := record.NewLayoutSchema(sche)
	ts, err := record.NewTableScan(tx, "mytable", recordLayout)
	if err != nil {
		t.Fatal(err)
	}
	idxLayout := index.NewIndexLayout(field)
	idx, err := index.NewBTreeIndex(tx, "myindex", idxLayout)
	if err != nil {
		t.Fatal(err)
	}

	vals := make([]schema.Constant, 0)
	cnt := 0
	for range 10 {
		for c := 'a'; c <= 'z'; c++ {
			vals = append(vals, schema.ConstantStr(fmt.Sprintf("%s%d", strings.Repeat(string(c), 5), cnt)))
			cnt++
		}
		for c := 'A'; c <= 'Z'; c++ {
			vals = append(vals, schema.ConstantStr(fmt.Sprintf("%s%d", strings.Repeat(string(c), 5), cnt)))
			cnt++
		}
	}

	for _, val := range vals {
		if err := ts.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := ts.SetVal("A", val); err != nil {
			t.Fatal(err)
		}
		if err := idx.Insert(val, ts.RID()); err != nil {
			t.Fatal(err)
		}
		idx := idx.(*index.BTreeIndex)

		d, err := idx.Dump()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Val: %v, Dump: %v\n", val, d)
	}

	for _, val := range vals {
		if err := idx.BeforeFirst(val); err != nil {
			t.Fatal(err)
		}
		ok, err := idx.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("no record found for %v", val)
			continue
		}

		gotRID, err := idx.DataRID()
		if err != nil {
			t.Fatal(err)
		}
		if err := ts.MoveToRID(gotRID); err != nil {
			t.Fatal(err)
		}
		gotVal, err := ts.Val("A")
		if err != nil {
			t.Fatal(err)
		}
		if !val.Equals(gotVal) {
			t.Errorf("expect %v, got %v", val, gotVal)
		}
	}

	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
}
