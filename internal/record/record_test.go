package record

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestLayout(t *testing.T) {
	t.Parallel()

	s := schema.NewSchema()
	s.AddInt32Field("A")
	s.AddStrField("B", 9)
	l := NewLayoutSchema(s)
	if offset, ok := l.Offset("A"); !ok || offset != 4 {
		t.Errorf("expected 0, got %d", offset)
	}
	if offset, ok := l.Offset("B"); !ok || offset != 8 {
		t.Errorf("expected 8, got %d", offset)
	}
}

func TestRecordPage(t *testing.T) {
	t.Parallel()

	fm, err := file.NewManager(t.TempDir(), 128)
	if err != nil {
		t.Fatal(err)
	}
	lm, err := log.NewManager(fm, "logfile")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	bm := buffer.NewManager(ctx, fm, lm, 8)

	tx, err := transaction.NewTransaction(ctx, bm, fm, lm)
	if err != nil {
		t.Fatal(err)
	}

	schema := schema.NewSchema()
	schema.AddInt32Field("A")
	schema.AddStrField("B", 9)

	layout := NewLayoutSchema(schema)
	blockID, err := tx.Append("testfile")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Pin(blockID); err != nil {
		t.Fatal(err)
	}
	rp, err := NewRecordPage(tx, blockID, layout)
	if err != nil {
		t.Fatal(err)
	}
	if err := rp.Format(); err != nil {
		t.Fatal(err)
	}

	slot, ok, err := rp.InsertAfter(-1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; ok; i++ {
		if err := rp.SetInt32(slot, "A", int32(i)); err != nil {
			t.Fatal(err)
		}
		s := fmt.Sprintf("rec%d", i)
		if err := rp.SetStr(slot, "B", s); err != nil {
			t.Fatal(err)
		}
		t.Logf("inserted at slot %d: A=%d, B=%s", slot, i, s)
		slot, ok, err = rp.InsertAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := rp.Delete(2); err != nil {
		t.Fatal(err)
	}
	if err := rp.Delete(4); err != nil {
		t.Fatal(err)
	}

	slot, ok, err = rp.NextAfter(-1)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0)
	for ok {
		a, err := rp.Int32(slot, "A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := rp.Str(slot, "B")
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("slot %d: A=%d, B=%s", slot, a, b))
		slot, ok, err = rp.NextAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}
	expected := []string{
		"slot 0: A=0, B=rec0",
		"slot 1: A=1, B=rec1",
		"slot 3: A=3, B=rec3",
		"slot 5: A=5, B=rec5",
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

func TestTableScan(t *testing.T) {
	t.Parallel()

	fm, err := file.NewManager(t.TempDir(), 128)
	if err != nil {
		t.Fatal(err)
	}
	lm, err := log.NewManager(fm, "logfile")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	bm := buffer.NewManager(ctx, fm, lm, 8)

	tx, err := transaction.NewTransaction(ctx, bm, fm, lm)
	if err != nil {
		t.Fatal(err)
	}

	schema := schema.NewSchema()
	schema.AddInt32Field("A")
	schema.AddStrField("B", 9)

	layout := NewLayoutSchema(schema)

	ts, err := NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := ts.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := ts.SetInt32("A", int32(i)); err != nil {
			t.Fatal(err)
		}
		s := fmt.Sprintf("rec%d", i)
		if err := ts.SetStr("B", s); err != nil {
			t.Fatal(err)
		}
		t.Logf("inserted: %v, {%v, %v}", ts.RID(), i, s)
	}

	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	ok, err := ts.Next()
	if err != nil {
		t.Fatal(err)
	}
	for ok {
		a, err := ts.Int32("A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := ts.Str("B")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("scanned: %v, {%v, %v}", ts.RID(), a, b)
		if a%2 == 0 {
			if err := ts.Delete(); err != nil {
				t.Fatal(err)
			}
			t.Logf("deleted: %v", ts.RID())
		}
		ok, err = ts.Next()
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	ok, err = ts.Next()
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0)
	for ok {
		a, err := ts.Int32("A")
		if err != nil {
			t.Fatal(err)
		}
		b, err := ts.Str("B")
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%v, {%v, %v}", ts.RID(), a, b))
		ok, err = ts.Next()
		if err != nil {
			t.Fatal(err)
		}
	}
	expected := []string{
		"RID{blockNum=0, slot=1}, {1, rec1}",
		"RID{blockNum=0, slot=3}, {3, rec3}",
		"RID{blockNum=0, slot=5}, {5, rec5}",
		"RID{blockNum=1, slot=1}, {7, rec7}",
		"RID{blockNum=1, slot=3}, {9, rec9}",
	}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}
