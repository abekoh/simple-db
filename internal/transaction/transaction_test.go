package transaction

import (
	"testing"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
)

func TestTransaction(t *testing.T) {
	fm, err := file.NewManager(t.TempDir(), 128)
	if err != nil {
		t.Fatal(err)
	}
	lm, err := log.NewManager(fm, "logfile")
	if err != nil {
		t.Fatal(err)
	}
	bm := buffer.NewManager(fm, lm, 8)

	tx1 := NewTransaction(bm, fm, lm)
	blockID := file.NewBlockID("testfile", 1)
	_, err = tx1.Pin(blockID)
	must(t, err)
	must(t, tx1.SetInt32(blockID, 80, 1, false))
	must(t, tx1.SetStr(blockID, 40, "one", false))
	must(t, tx1.Commit())

	tx2 := NewTransaction(bm, fm, lm)
	_, err = tx2.Pin(blockID)
	beforeIntVal, err := tx2.Int32(blockID, 80)
	must(t, err)
	beforeStrVal, err := tx2.Str(blockID, 40)
	must(t, err)
	if beforeIntVal != 1 {
		t.Errorf("expected 1, got %d", beforeIntVal)
	}
	if beforeStrVal != "one" {
		t.Errorf("expected one, got %s", beforeStrVal)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
