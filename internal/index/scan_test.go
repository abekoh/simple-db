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
		idx1, err := cfg.Initializer(tx, "I", idxLayout)
		if err != nil {
			t.Fatal(err)
		}

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
		idx2, err := cfg.Initializer(tx, "I", idxLayout)
		if err != nil {
			t.Fatal(err)
		}
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

		lhsSche := schema.NewSchema()
		lhsSche.AddStrField("A", 9)
		fieldB := schema.NewField(schema.Varchar, 9)
		lhsSche.AddField("B", fieldB)
		lhsLayout := record.NewLayoutSchema(lhsSche)
		lhsTableS1, err := record.NewTableScan(tx, "T1", lhsLayout)
		if err != nil {
			t.Fatal(err)
		}
		if err := lhsTableS1.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		n := 5
		for i := 0; i < n; i++ {
			if err := lhsTableS1.Insert(); err != nil {
				t.Fatal(err)
			}
			if err := lhsTableS1.SetStr("A", fmt.Sprintf("aaa%d", i)); err != nil {
				t.Fatal(err)
			}
			if err := lhsTableS1.SetStr("B", fmt.Sprintf("bbb%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := lhsTableS1.Close(); err != nil {
			t.Fatal(err)
		}

		rhsSche := schema.NewSchema()
		rhsSche.AddField("B", fieldB)
		rhsSche.AddStrField("C", 9)
		rhsLayout := record.NewLayoutSchema(rhsSche)
		rhsTableS1, err := record.NewTableScan(tx, "T2", rhsLayout)
		if err != nil {
			t.Fatal(err)
		}
		idxLayout := index.NewIndexLayout(fieldB)
		rhsIdx1, err := cfg.Initializer(tx, "I", idxLayout)
		if err != nil {
			t.Fatal(err)
		}
		if err := rhsTableS1.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n+5; i++ {
			if err := rhsTableS1.Insert(); err != nil {
				t.Fatal(err)
			}
			rec := schema.ConstantStr(fmt.Sprintf("bbb%d", i))
			if err := rhsTableS1.SetVal("B", rec); err != nil {
				t.Fatal(err)
			}
			if err := rhsIdx1.Insert(rec, rhsTableS1.RID()); err != nil {
				t.Fatal(err)
			}
			if err := rhsTableS1.SetStr("C", fmt.Sprintf("ccc%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rhsTableS1.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rhsIdx1.Close(); err != nil {
			t.Fatal(err)
		}

		lhsTableS2, err := record.NewTableScan(tx, "T1", lhsLayout)
		if err != nil {
			t.Fatal(err)
		}
		rhsTableS2, err := record.NewTableScan(tx, "T2", rhsLayout)
		if err != nil {
			t.Fatal(err)
		}
		idx2, err := cfg.Initializer(tx, "I", idxLayout)
		if err != nil {
			t.Fatal(err)
		}
		joinS, err := index.NewJoinScan(lhsTableS2, rhsTableS2, idx2, "B")
		if err != nil {
			t.Fatal(err)
		}
		prjS := query.NewProjectScan(joinS, "A", "B", "C")
		got := make([]string, 0, n)
		for {
			ok, err := prjS.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			a, err := prjS.Str("A")
			if err != nil {
				t.Fatal(err)
			}
			b, err := prjS.Str("B")
			if err != nil {
				t.Fatal(err)
			}
			c, err := prjS.Str("C")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%s, %s, %s", a, b, c))
		}
		if len(got) != n {
			t.Errorf("got %d, want %d", len(got), n)
		}
		expected := `aaa0, bbb0, ccc0
aaa1, bbb1, ccc1
aaa2, bbb2, ccc2
aaa3, bbb3, ccc3
aaa4, bbb4, ccc4`
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
