package query_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/server"
)

func TestProductScan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := server.NewSimpleDB(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sche1 := schema.NewSchema()
	sche1.AddInt32Field("A")
	sche1.AddStrField("B", 9)
	layout1 := record.NewLayoutSchema(sche1)
	ts1, err := record.NewTableScan(tx, "T1", layout1)
	if err != nil {
		t.Fatal(err)
	}

	sche2 := schema.NewSchema()
	sche2.AddInt32Field("C")
	sche2.AddStrField("D", 9)
	layout2 := record.NewLayoutSchema(sche2)
	ts2, err := record.NewTableScan(tx, "T2", layout2)
	if err != nil {
		t.Fatal(err)
	}

	if err := ts1.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := ts1.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := ts1.SetInt32("A", int32(i)); err != nil {
			t.Fatal(err)
		}
		if err := ts1.SetStr("B", fmt.Sprintf("aaa%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	if err := ts2.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := ts2.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := ts2.SetInt32("C", int32(i)); err != nil {
			t.Fatal(err)
		}
		if err := ts2.SetStr("D", fmt.Sprintf("bbb%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := ts2.Close(); err != nil {
		t.Fatal(err)
	}

}

func TestScan(t *testing.T) {
	t.Parallel()
	t.Run("Scan1", func(t *testing.T) {
		t.Parallel()
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
		sche.AddInt32Field("A")
		sche.AddStrField("B", 9)
		layout := record.NewLayoutSchema(sche)
		scan1, err := record.NewTableScan(tx, "T1", layout)
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
			if err := scan1.SetInt32("A", int32(i)); err != nil {
				t.Fatal(err)
			}
			if err := scan1.SetStr("B", fmt.Sprintf("rec%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := scan1.Close(); err != nil {
			t.Fatal(err)
		}

		scan2, err := record.NewTableScan(tx, "T2", layout)
		if err != nil {
			t.Fatal(err)
		}
		term := query.NewTerm(schema.FieldName("A"), schema.ConstantInt32(10))
		pred := query.NewPredicate(term)
		if pred.String() != "A=10" {
			t.Fatalf("unexpected string: %s", pred.String())
		}
		// TODO
		_ = scan2

		ts1p, err := record.NewTableScan(tx, "T1", layout)
		if err != nil {
			t.Fatal(err)
		}
		ts2p, err := record.NewTableScan(tx, "T2", layout)
		if err != nil {
			t.Fatal(err)
		}
		ps := query.NewProductScan(ts1p, ts2p)
		got := make([]string, 0, 200)
		for {
			ok, err := ps.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			a, err := ps.Int32("A")
			if err != nil {
				t.Fatal(err)
			}
			b, err := ps.Str("B")
			if err != nil {
				t.Fatal(err)
			}
			c, err := ps.Int32("C")
			if err != nil {
				t.Fatal(err)
			}
			d, err := ps.Str("D")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("{%d, %s, %d, %s}", a, b, c, d))
		}
		if len(got) != 200*200 {
			t.Fatalf("got %d, want %d", len(got), 200*200)
		}
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

}
