package plan_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/query"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestProductPlan(t *testing.T) {
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

	sche1 := schema.NewSchema()
	sche1.AddInt32Field("A")
	sche1.AddStrField("B", 9)
	if err := db.MetadataMgr().CreateTable("T1", sche1, tx); err != nil {
		t.Fatal(err)
	}
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
	if err := db.MetadataMgr().CreateTable("T2", sche2, tx); err != nil {
		t.Fatal(err)
	}
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

	tablePlan1, err := plan.NewTablePlan("T1", tx, db.MetadataMgr())
	if err != nil {
		t.Fatal(err)
	}
	tablePlan2, err := plan.NewTablePlan("T2", tx, db.MetadataMgr())
	if err != nil {
		t.Fatal(err)
	}
	prodPlan := plan.NewMultiBufferProductPlan(tx, tablePlan1, tablePlan2)
	selectPlan := plan.NewSelectPlan(prodPlan, query.NewPredicate(query.NewTerm(schema.FieldName("A"), schema.FieldName("C"))))
	projectPlan := plan.NewProjectPlan(selectPlan, []schema.FieldName{"B", "D"})

	s, err := projectPlan.Open()
	if err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0, n*n)
	for {
		ok, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		b, err := s.Str("B")
		if err != nil {
			t.Fatal(err)
		}
		d, err := s.Str("D")
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
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
