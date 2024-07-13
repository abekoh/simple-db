package index_test

import (
	"context"
	"fmt"
	"strings"
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
		idx1 := cfg.Initializer(tx, "I", idxLayout)

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
			if err := idx1.Insert(rec, scan1.RID()); err != nil {
				t.Fatal(err)
			}
		}
		if err := scan1.Close(); err != nil {
			t.Fatal(err)
		}
		if err := idx1.Close(); err != nil {
			t.Fatal(err)
		}

		scan2, err := record.NewTableScan(tx, "T", layout)
		if err != nil {
			t.Fatal(err)
		}
		idx2 := cfg.Initializer(tx, "I", idxLayout)
		scan3, err := index.NewSelectScan(scan2, idx2, schema.ConstantStr("rec100"))
		if err != nil {
			t.Fatal(err)
		}
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
	t.Run("TableScan*2 -> IndexProductScan -> ProjectScan", func(t *testing.T) {
		ctx := context.Background()
		db, err := simpledb.New(ctx, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.NewTx(ctx)
		if err != nil {
			t.Fatal(err)
		}

		sche1 := schema.NewSchema()
		sche1.AddStrField("A", 9)
		fieldB := schema.NewField(schema.Varchar, 9)
		sche1.AddField("B", fieldB)
		layout1 := record.NewLayoutSchema(sche1)
		ts1, err := record.NewTableScan(tx, "T1", layout1)
		if err != nil {
			t.Fatal(err)
		}
		if err := ts1.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		n := 5
		for i := 0; i < n; i++ {
			if err := ts1.Insert(); err != nil {
				t.Fatal(err)
			}
			if err := ts1.SetStr("A", fmt.Sprintf("aaa%d", i)); err != nil {
				t.Fatal(err)
			}
			if err := ts1.SetStr("B", fmt.Sprintf("bbb%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := ts1.Close(); err != nil {
			t.Fatal(err)
		}

		sche2 := schema.NewSchema()
		sche2.AddField("B", fieldB)
		sche2.AddStrField("C", 9)
		layout2 := record.NewLayoutSchema(sche2)
		ts2, err := record.NewTableScan(tx, "T2", layout2)
		if err != nil {
			t.Fatal(err)
		}
		idxLayout := index.NewIndexLayout(fieldB)
		idx1 := cfg.Initializer(tx, "I", idxLayout)
		if err := ts2.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			if err := ts2.Insert(); err != nil {
				t.Fatal(err)
			}
			rec := schema.ConstantStr(fmt.Sprintf("bbb%d", i))
			if err := ts2.SetVal("B", rec); err != nil {
				t.Fatal(err)
			}
			if err := idx1.Insert(rec, ts2.RID()); err != nil {
				t.Fatal(err)
			}
			if err := ts2.SetStr("C", fmt.Sprintf("ccc%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := ts2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := idx1.Close(); err != nil {
			t.Fatal(err)
		}

		s1p, err := record.NewTableScan(tx, "T1", layout1)
		if err != nil {
			t.Fatal(err)
		}
		s2p, err := record.NewTableScan(tx, "T2", layout2)
		if err != nil {
			t.Fatal(err)
		}
		idx2 := cfg.Initializer(tx, "I", idxLayout)
		joinS, err := index.NewJoinScan(s1p, s2p, idx2, "B")
		if err != nil {
			t.Fatal(err)
		}
		prjS := query.NewProjectScan(joinS, "A", "B", "C")
		got := make([]string, 0, n*n)
		for {
			ok, err := prjS.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			b, err := prjS.Str("B")
			if err != nil {
				t.Fatal(err)
			}
			d, err := prjS.Str("D")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%s, %s", b, d))
		}
		if len(got) != n {
			t.Errorf("got %d, want %d", len(got), n)
		}
		expected := `bbb0, ddd0
bbb1, ddd1
bbb2, ddd2
bbb3, ddd3
bbb4, ddd4`
		if strings.Join(got, "\n") != expected {
			t.Errorf("got %s, want %s", strings.Join(got, "\n"), expected)
		}
		if err := prjS.Close(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}
