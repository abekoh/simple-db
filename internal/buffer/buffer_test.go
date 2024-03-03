package buffer

import (
	"testing"
	"time"

	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
)

func mustPin(t *testing.T, bm *Manager, blockID file.BlockID) *Buffer {
	t.Helper()
	buf, err := bm.Pin(blockID)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func assertAvailableNum(t *testing.T, bm *Manager, expected int) {
	t.Helper()
	if bm.AvailableNum() != expected {
		t.Errorf("expected %d, got %d", expected, bm.AvailableNum())
	}
}

func TestBufferManager(t *testing.T) {
	t.Parallel()
	t.Run("Pin", func(t *testing.T) {
		t.Parallel()
		fm, err := file.NewManager(t.TempDir(), 128)
		if err != nil {
			t.Fatal(err)
		}
		lm, err := log.NewManager(fm, "logfile")
		if err != nil {
			t.Fatal(err)
		}
		bm := NewManager(fm, lm, 3, WithMaxWaitTime(10*time.Millisecond))

		assertAvailableNum(t, bm, 3)

		bufs := make([]*Buffer, 6)
		bufs[0] = mustPin(t, bm, file.NewBlockID("testfile", 0))
		assertAvailableNum(t, bm, 2)

		bufs[1] = mustPin(t, bm, file.NewBlockID("testfile", 1))
		bufs[2] = mustPin(t, bm, file.NewBlockID("testfile", 2))
		assertAvailableNum(t, bm, 0)

		bufs[3] = mustPin(t, bm, file.NewBlockID("testfile", 0))
		bm.Unpin(bufs[1])
		bufs[1] = nil
		assertAvailableNum(t, bm, 1)

		bufs[3] = mustPin(t, bm, file.NewBlockID("testfile", 0))
		bufs[4] = mustPin(t, bm, file.NewBlockID("testfile", 1))
		assertAvailableNum(t, bm, 0)

		bufs[5] = mustPin(t, bm, file.NewBlockID("testfile", 3))

	})
}
