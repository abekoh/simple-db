package buffer

import (
	"testing"

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
		bm := NewManager(fm, lm, 3)

		if bm.AvailableNum() != 3 {
			t.Errorf("expected 0, got %d", bm.AvailableNum())
		}
		bufs := make([]*Buffer, 6)
		bufs[0] = mustPin(t, bm, file.NewBlockID("testfile", 0))
		if bm.AvailableNum() != 2 {
			t.Errorf("expected 0, got %d", bm.AvailableNum())
		}
		bufs[1] = mustPin(t, bm, file.NewBlockID("testfile", 1))
		bufs[2] = mustPin(t, bm, file.NewBlockID("testfile", 2))
		if bm.AvailableNum() != 0 {
			t.Errorf("expected 0, got %d", bm.AvailableNum())
		}
		bm.Unpin(bufs[1])
		bufs[1] = nil
		if bm.AvailableNum() != 1 {
			t.Errorf("expected 1, got %d", bm.AvailableNum())
		}
		//bufs[3] = mustPin(t, bm, file.NewBlockID("testfile", 0))
		//bufs[4] = mustPin(t, bm, file.NewBlockID("testfile", 1))
		//if bm.AvailableNum() != 0 {
		//	t.Errorf("expected 0, got %d", bm.AvailableNum())
		//}
	})
}
