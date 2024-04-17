package record

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestLayout(t *testing.T) {
	t.Parallel()

	s := NewSchema()
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
	bm := buffer.NewManager(fm, lm, 8)

	tx, err := transaction.NewTransaction(bm, fm, lm)
	if err != nil {
		t.Fatal(err)
	}

	schema := NewSchema()
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

	t.Log("Filling the page with random records.")
	slot, err := rp.InsertAfter(-1)
	if err != nil {
		t.Fatal(err)
	}
	for slot >= 0 {
		n := rand.Int32N(50)
		if err := rp.SetInt32(slot, "A", n); err != nil {
			t.Fatal(err)
		}
		s := fmt.Sprintf("regwoiwaogihwaec%d", n)
		if err := rp.SetStr(slot, "B", s); err != nil {
			t.Fatal(err)
		}
		t.Logf("inserting into slot %d : { %d, %s }", slot, n, s)
		slot, err = rp.InsertAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}
}
