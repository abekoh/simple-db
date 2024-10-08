package transaction

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
)

func TestTransaction(t *testing.T) {
	t.Parallel()
	t.Run("Transaction", func(t *testing.T) {
		CleanupLockTable(t)
		must := func(t *testing.T, err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}

		slog.SetLogLoggerLevel(slog.LevelDebug)
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

		tx1, err := NewTransaction(ctx, bm, fm, lm)
		must(t, err)
		blockID := file.NewBlockID("testfile", 1)
		_, err = tx1.Pin(blockID)
		must(t, err)
		must(t, tx1.SetInt32(blockID, 80, 1, false))
		must(t, tx1.SetStr(blockID, 40, "one", false))
		must(t, tx1.Commit())

		tx2, err := NewTransaction(ctx, bm, fm, lm)
		must(t, err)
		_, err = tx2.Pin(blockID)
		must(t, err)
		beforeTx2IntVal, err := tx2.Int32(blockID, 80)
		must(t, err)
		beforeTx2StrVal, err := tx2.Str(blockID, 40)
		must(t, err)
		if beforeTx2IntVal != 1 {
			t.Errorf("expected 1, got %d", beforeTx2IntVal)
		}
		if beforeTx2StrVal != "one" {
			t.Errorf("expected one, got %s", beforeTx2StrVal)
		}
		must(t, tx2.SetInt32(blockID, 80, beforeTx2IntVal+1, true))
		must(t, tx2.SetStr(blockID, 40, beforeTx2StrVal+"!", true))
		must(t, tx2.Commit())

		tx3, err := NewTransaction(ctx, bm, fm, lm)
		must(t, err)
		_, err = tx3.Pin(blockID)
		must(t, err)
		beforeTx3IntVal, err := tx3.Int32(blockID, 80)
		must(t, err)
		beforeTx3StrVal, err := tx3.Str(blockID, 40)
		must(t, err)
		if beforeTx3IntVal != 2 {
			t.Errorf("expected 2, got %d", beforeTx3IntVal)
		}
		if beforeTx3StrVal != "one!" {
			t.Errorf("expected one!, got %s", beforeTx3StrVal)
		}
		must(t, tx3.SetInt32(blockID, 80, 9999, true))
		must(t, tx3.SetStr(blockID, 40, "two", true))
		must(t, tx3.Rollback())

		tx4, err := NewTransaction(ctx, bm, fm, lm)
		must(t, err)
		_, err = tx4.Pin(blockID)
		must(t, err)
		beforeTx4IntVal, err := tx4.Int32(blockID, 80)
		must(t, err)
		beforeTx4StrVal, err := tx4.Str(blockID, 40)
		must(t, err)
		if beforeTx4IntVal != 2 {
			t.Errorf("expected 2, got %d", beforeTx4IntVal)
		}
		if beforeTx4StrVal != "one!" {
			t.Errorf("expected one!, got %s", beforeTx4StrVal)

		}
		must(t, tx4.Commit())

		records := make([]string, 0)
		for raw := range lm.Iterator() {
			r := NewLogRecord(raw)
			records = append(records, r.String())
		}
		if !reflect.DeepEqual(records, []string{
			"<COMMIT 4>",
			"<START 4>",
			"<ROLLBACK 3>",
			"<SETSTR 3 testfile:1 40 one!>",
			"<SETINT32 3 testfile:1 80 2>",
			"<START 3>",
			"<COMMIT 2>",
			"<SETSTR 2 testfile:1 40 one>",
			"<SETINT32 2 testfile:1 80 1>",
			"<START 2>",
			"<COMMIT 1>",
			"<START 1>",
		}) {

		}
	})
	t.Run("Concurrency", func(t *testing.T) {
		CleanupLockTable(t)
		slog.SetLogLoggerLevel(slog.LevelDebug)
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

		var g errgroup.Group
		g.Go(func() error {
			txA, err := NewTransaction(ctx, bm, fm, lm)
			if err != nil {
				return fmt.Errorf("failed txA: %w", err)
			}
			blockID1 := file.NewBlockID("testfile", 1)
			blockID2 := file.NewBlockID("testfile", 2)
			_, err = txA.Pin(blockID1)
			if err != nil {
				return fmt.Errorf("failed txA: %w", err)
			}
			_, err = txA.Pin(blockID2)
			if err != nil {
				return fmt.Errorf("failed txA: %w", err)
			}
			t.Logf("txA: request sLock %s", blockID1)
			_, err = txA.Int32(blockID1, 0)
			if err != nil {
				return fmt.Errorf("failed txA: %w", err)
			}
			t.Logf("txA: receive sLock %s", blockID1)
			time.Sleep(1 * time.Second)
			t.Logf("txA: request sLock %s", blockID2)
			_, err = txA.Int32(blockID2, 0)
			if err != nil {
				return fmt.Errorf("failed txA: %w", err)
			}
			t.Logf("txA: receive sLock %s", blockID2)
			if err := txA.Commit(); err != nil {
				return fmt.Errorf("failed txA: %w", err)
			}
			t.Log("txA: commit")
			return nil
		})
		g.Go(func() error {
			txB, err := NewTransaction(ctx, bm, fm, lm)
			if err != nil {
				return fmt.Errorf("failed txB: %w", err)
			}
			blockID1 := file.NewBlockID("testfile", 1)
			blockID2 := file.NewBlockID("testfile", 2)
			_, err = txB.Pin(blockID1)
			if err != nil {
				return fmt.Errorf("failed txB: %w", err)
			}
			_, err = txB.Pin(blockID2)
			if err != nil {
				return fmt.Errorf("failed txB: %w", err)
			}
			t.Logf("txB: request xLock %s", blockID2)
			if err := txB.SetInt32(blockID2, 0, 0, false); err != nil {
				return fmt.Errorf("failed txB: %w", err)
			}
			t.Logf("txB: receive xLock %s", blockID2)
			time.Sleep(1 * time.Second)
			t.Logf("txB: request sLock %s", blockID1)
			_, err = txB.Int32(blockID1, 0)
			if err != nil {
				return fmt.Errorf("failed txB: %w", err)
			}
			t.Logf("txB: receive sLock %s", blockID1)
			if err := txB.Commit(); err != nil {
				return fmt.Errorf("failed txB: %w", err)
			}
			t.Log("txB: commit")
			return nil
		})
		g.Go(func() error {
			txC, err := NewTransaction(ctx, bm, fm, lm)
			if err != nil {
				return fmt.Errorf("failed txC: %w", err)
			}
			blockID1 := file.NewBlockID("testfile", 1)
			blockID2 := file.NewBlockID("testfile", 2)
			_, err = txC.Pin(blockID1)
			if err != nil {
				return fmt.Errorf("failed txC: %w", err)
			}
			_, err = txC.Pin(blockID2)
			if err != nil {
				return fmt.Errorf("failed txC: %w", err)
			}
			time.Sleep(500 * time.Millisecond)
			t.Logf("txC: request xLock %s", blockID1)
			if err := txC.SetInt32(blockID1, 0, 0, false); err != nil {
				return fmt.Errorf("failed txC: %w", err)
			}
			t.Logf("txC: receive xLock %s", blockID1)
			time.Sleep(1 * time.Second)
			t.Logf("txC: request sLock %s", blockID2)
			_, err = txC.Int32(blockID2, 0)
			if err != nil {
				return fmt.Errorf("failed txC: %w", err)
			}
			t.Logf("txC: receive sLock %s", blockID2)
			if err := txC.Commit(); err != nil {
				return fmt.Errorf("failed txC: %w", err)
			}
			t.Log("txC: commit")
			return nil
		})
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("Concurrency sLock after xLock", func(t *testing.T) {
		CleanupLockTable(t)
		slog.SetLogLoggerLevel(slog.LevelDebug)
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

		tx, err := NewTransaction(ctx, bm, fm, lm)
		if err != nil {
			t.Fatal(err)
		}
		blockID1 := file.NewBlockID("testfile", 1)
		_, err = tx.Pin(blockID1)
		if err != nil {
			t.Fatal(err)
		}
		err = tx.SetInt32(blockID1, 0, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Int32(blockID1, 0)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestTransaction_Recover(t *testing.T) {
	CleanupLockTable(t)
	slog.SetLogLoggerLevel(slog.LevelDebug)
	tmpDir := t.TempDir()
	blockID0 := file.NewBlockID("testfile", 0)
	blockID1 := file.NewBlockID("testfile", 1)
	assertValues := func(t *testing.T, expected string, fm *file.Manager) {
		p0 := file.NewPage(fm.BlockSize())
		p1 := file.NewPage(fm.BlockSize())
		if err := fm.Read(blockID0, p0); err != nil {
			t.Fatal(err)
		}
		if err := fm.Read(blockID1, p1); err != nil {
			t.Fatal(err)
		}
		pos := int32(0)
		var sb strings.Builder
		for i := 0; i < 6; i++ {
			sb.WriteString(fmt.Sprintf("%d ", p0.Int32(pos)))
			sb.WriteString(fmt.Sprintf("%d ", p1.Int32(pos)))
			pos += 4
		}
		sb.WriteString(p0.Str(30) + " ")
		sb.WriteString(p1.Str(30) + " ")
		if sb.String() != expected {
			t.Errorf("expected %s, got %s", expected, sb.String())
		}
	}

	fm1, err := file.NewManager(tmpDir, 400)
	if err != nil {
		t.Fatal(err)
	}
	lm1, err := log.NewManager(fm1, "logfile")
	if err != nil {
		t.Fatal(err)
	}
	ctx1, cancel := context.WithCancel(context.Background())
	bm1 := buffer.NewManager(ctx1, fm1, lm1, 8)

	tx1, err := NewTransaction(ctx1, bm1, fm1, lm1)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := NewTransaction(ctx1, bm1, fm1, lm1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx1.Pin(blockID0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Pin(blockID1)
	if err != nil {
		t.Fatal(err)
	}

	// initialize
	pos := int32(0)
	for i := 0; i < 6; i++ {
		if err := tx1.SetInt32(blockID0, pos, pos, false); err != nil {
			t.Fatal(err)
		}
		if err := tx2.SetInt32(blockID1, pos, pos, false); err != nil {
			t.Fatal(err)
		}
		pos += 4
	}
	if err := tx1.SetStr(blockID0, 30, "abc", false); err != nil {
		t.Fatal(err)
	}
	if err := tx2.SetStr(blockID1, 30, "def", false); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	assertValues(t, "0 0 4 4 8 8 12 12 16 16 20 20 abc def ", fm1)

	// modify
	tx3, err := NewTransaction(ctx1, bm1, fm1, lm1)
	if err != nil {
		t.Fatal(err)
	}
	tx4, err := NewTransaction(ctx1, bm1, fm1, lm1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx3.Pin(blockID0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx4.Pin(blockID1)
	if err != nil {
		t.Fatal(err)
	}
	pos = int32(0)
	for i := 0; i < 6; i++ {
		if err := tx3.SetInt32(blockID0, pos, pos+100, true); err != nil {
			t.Fatal(err)
		}
		if err := tx4.SetInt32(blockID1, pos, pos+100, true); err != nil {
			t.Fatal(err)
		}
		pos += 4
	}
	if err := tx3.SetStr(blockID0, 30, "uvw", true); err != nil {
		t.Fatal(err)
	}
	if err := tx4.SetStr(blockID1, 30, "xyz", true); err != nil {
		t.Fatal(err)
	}
	if err := bm1.FlushAll(3); err != nil {
		t.Fatal(err)
	}
	if err := bm1.FlushAll(4); err != nil {
		t.Fatal(err)
	}
	assertValues(t, "100 100 104 104 108 108 112 112 116 116 120 120 uvw xyz ", fm1)

	// rollback tx3
	if err := tx3.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertValues(t, "0 100 4 104 8 108 12 112 16 116 20 120 abc xyz ", fm1)

	cancel()

	fm2, err := file.NewManager(tmpDir, 400)
	if err != nil {
		t.Fatal(err)
	}
	lm2, err := log.NewManager(fm2, "logfile")
	if err != nil {
		t.Fatal(err)
	}
	ctx2 := context.Background()
	bm2 := buffer.NewManager(ctx2, fm2, lm2, 8)

	// recover (rollback tx4)
	tx5, err := NewTransaction(ctx2, bm2, fm2, lm2)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx5.Recover(); err != nil {
		t.Fatal(err)
	}
	assertValues(t, "0 0 4 4 8 8 12 12 16 16 20 20 abc def ", fm2)
}
