package query_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func TestProductScan(t *testing.T) {
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
	n := 5
	for i := 0; i < n; i++ {
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
	for i := 0; i < n; i++ {
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

	ts1p, err := record.NewTableScan(tx, "T1", layout1)
	if err != nil {
		t.Fatal(err)
	}
	ts2p, err := record.NewTableScan(tx, "T2", layout2)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := query.NewProductScan(ts1p, ts2p)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, n*n)
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
	if len(got) != n*n {
		t.Errorf("got %d, want %d", len(got), n*n)
	}
	expected := `{0, aaa0, 0, bbb0}
{0, aaa0, 1, bbb1}
{0, aaa0, 2, bbb2}
{0, aaa0, 3, bbb3}
{0, aaa0, 4, bbb4}
{1, aaa1, 0, bbb0}
{1, aaa1, 1, bbb1}
{1, aaa1, 2, bbb2}
{1, aaa1, 3, bbb3}
{1, aaa1, 4, bbb4}
{2, aaa2, 0, bbb0}
{2, aaa2, 1, bbb1}
{2, aaa2, 2, bbb2}
{2, aaa2, 3, bbb3}
{2, aaa2, 4, bbb4}
{3, aaa3, 0, bbb0}
{3, aaa3, 1, bbb1}
{3, aaa3, 2, bbb2}
{3, aaa3, 3, bbb3}
{3, aaa3, 4, bbb4}
{4, aaa4, 0, bbb0}
{4, aaa4, 1, bbb1}
{4, aaa4, 2, bbb2}
{4, aaa4, 3, bbb3}
{4, aaa4, 4, bbb4}`
	if strings.Join(got, "\n") != expected {
		t.Errorf("got %s, want %s", strings.Join(got, "\n"), expected)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestScan(t *testing.T) {
	t.Run("TableScan -> SelectScan -> ProjectScan", func(t *testing.T) {
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
		sche.AddStrField("B", 9)
		layout := record.NewLayoutSchema(sche)
		scan1, err := record.NewTableScan(tx, "T", layout)
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
			if err := scan1.SetInt32("A", int32(i/10)); err != nil {
				t.Fatal(err)
			}
			if err := scan1.SetStr("B", fmt.Sprintf("rec%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := scan1.Close(); err != nil {
			t.Fatal(err)
		}

		scan2, err := record.NewTableScan(tx, "T", layout)
		if err != nil {
			t.Fatal(err)
		}
		term := query.NewTerm(schema.FieldName("A"), schema.ConstantInt32(10))
		pred := query.NewPredicate(term)
		if pred.String() != "A=10" {
			t.Fatalf("unexpected string: %s", pred.String())
		}
		scan3 := query.NewSelectScan(scan2, pred)
		scan4 := query.NewProjectScan(scan3, "B")
		got := make([]string, 0, 10)
		for {
			ok, err := scan4.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			b, err := scan4.Str("B")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, b)
		}
		if len(got) != 10 {
			t.Errorf("got %d, want %d", len(got), 10)
		}
		expected := []string{"rec100", "rec101", "rec102", "rec103", "rec104", "rec105", "rec106", "rec107", "rec108", "rec109"}
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("got %v, want %v", got, expected)
		}
		if err := scan4.Close(); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("TableScan -> ProductScan -> SelectScan -> ProjectScan", func(t *testing.T) {
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
		sche1.AddInt32Field("A")
		sche1.AddStrField("B", 9)
		layout1 := record.NewLayoutSchema(sche1)
		us1, err := record.NewTableScan(tx, "T1", layout1)
		if err != nil {
			t.Fatal(err)
		}
		if err := us1.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		n := 5
		for i := 0; i < n; i++ {
			if err := us1.Insert(); err != nil {
				t.Fatal(err)
			}
			if err := us1.SetInt32("A", int32(i)); err != nil {
				t.Fatal(err)
			}
			if err := us1.SetStr("B", fmt.Sprintf("bbb%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := us1.Close(); err != nil {
			t.Fatal(err)
		}

		sche2 := schema.NewSchema()
		sche2.AddInt32Field("C")
		sche2.AddStrField("D", 9)
		layout2 := record.NewLayoutSchema(sche2)
		us2, err := record.NewTableScan(tx, "T2", layout2)
		if err != nil {
			t.Fatal(err)
		}
		if err := us2.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			if err := us2.Insert(); err != nil {
				t.Fatal(err)
			}
			if err := us2.SetInt32("C", int32(i)); err != nil {
				t.Fatal(err)
			}
			if err := us2.SetStr("D", fmt.Sprintf("ddd%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := us2.Close(); err != nil {
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
		prodS, err := query.NewProductScan(s1p, s2p)
		if err != nil {
			t.Fatal(err)
		}
		term := query.NewTerm(schema.FieldName("A"), schema.FieldName("C"))
		pred := query.NewPredicate(term)
		ss := query.NewSelectScan(prodS, pred)
		prjS := query.NewProjectScan(ss, "B", "D")
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
			t.Errorf("got %d, want %d", len(got), n*n)
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
