package log

import (
	"fmt"
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
		for i := 0; i < 100000; i++ {
			_, err := lm.Append([]byte(fmt.Sprintf("%04d", i)))
			if err != nil {
				t.Fatal(err)
			}
		}
		count := 0
		for r := range lm.Iterator() {
			if string(r) != fmt.Sprintf("%04d", 100000-count-1) {
				t.Errorf("expected %04d, got %s", 100000-count-1, r)
			}
			count++
		}
		if count != 100000 {
			t.Errorf("expected 100000, got %d", count)
		}
	})
}
