package index_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/abekoh/simple-db/internal/index"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func TestIndexScan(t *testing.T) {
	cfg := index.ConfigHash
	t.Run("TableScan -> IndexSelectScan -> ProjectScan", func(t *testing.T) {
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
		fieldB := schema.NewField(schema.Varchar, 9)
		sche.AddField("B", fieldB)
		layout := record.NewLayoutSchema(sche)
		scan1, err := record.NewTableScan(tx, "T", layout)
		if err != nil {
			t.Fatal(err)
		}

		idxLayout := index.NewIndexLayout(fieldB)
		idx := cfg.Initializer(tx, "I", idxLayout)

		if err := scan1.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 200; i++ {
			if err := scan1.Insert(); err != nil {
				t.Fatal(err)
			}
			rec := schema.ConstantStr(fmt.Sprintf("rec%d", i))
			if err := scan1.SetVal("B", rec); err != nil {
				t.Fatal(err)
			}
			if err := idx.Insert(rec, scan1.RID()); err != nil {
				t.Fatal(err)
			}
		}
		if err := scan1.Close(); err != nil {
			t.Fatal(err)
		}
		if err := idx.Close(); err != nil {
			t.Fatal(err)
		}

		scan2, err := record.NewTableScan(tx, "T", layout)
		if err != nil {
			t.Fatal(err)
		}
		scan3 := index.NewSelectScan(scan2, idx, schema.ConstantStr("rec100"))
		scan4 := query.NewProjectScan(scan3, "B")

		ok, err := scan4.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("no records")
		}
		b, err := scan4.Str("B")
		if err != nil {
			t.Fatal(err)
		}
		if b != "rec100" {
			t.Fatalf("unexpected value: %s", b)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}
