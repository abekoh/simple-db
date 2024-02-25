package log

import (
	"testing"

	"github.com/abekoh/simple-db/internal/file"
)

func TestManager(t *testing.T) {
	t.Parallel()
	t.Run("Append", func(t *testing.T) {
		t.Parallel()
		fm, err := file.NewManager(t.TempDir(), 400)
		if err != nil {
			t.Fatal(err)
		}
		lm, err := NewManager(fm, "logfile")
		if err != nil {
			t.Fatal(err)
		}
		lsn, err := lm.Append([]byte("abcd"))
		if err != nil {
			t.Fatal(err)
		}
		if lsn != 1 {
			t.Errorf("expected 0, got %d", lsn)
		}
		for r := range lm.Iterator() {
			t.Logf("%s\n", string(r))
		}
	})
}
