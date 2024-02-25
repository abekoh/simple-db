package log

import (
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/file"
)

func TestManager(t *testing.T) {
	t.Parallel()
	t.Run("Append", func(t *testing.T) {
		t.Parallel()
		fm, err := file.NewManager(t.TempDir(), 128)
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
		var logs []string
		for r := range lm.Iterator() {
			logs = append(logs, string(r))
		}
		if !reflect.DeepEqual(logs, []string{"abcd"}) {
			t.Errorf("expected [abcd], got %v", logs)
		}
	})
	t.Run("Append many", func(t *testing.T) {
		t.Parallel()
		fm, err := file.NewManager(t.TempDir(), 128)
		if err != nil {
			t.Fatal(err)
		}
		lm, err := NewManager(fm, "logfile")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10000; i++ {
			_, err := lm.Append([]byte("abcd"))
			if err != nil {
				t.Fatal(err)
			}
		}
		count := 0
		for r := range lm.Iterator() {
			count++
			if string(r) != "abcd" {
				t.Errorf("expected abcd, got %s", string(r))
			}
		}
		if count != 10000 {
			t.Errorf("expected 10000, got %d", count)
		}
	})
}
