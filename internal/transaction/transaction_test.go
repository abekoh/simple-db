package transaction

import (
	"fmt"
	"log/slog"
	"reflect"
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
		bm := buffer.NewManager(fm, lm, 8)

		tx1, err := NewTransaction(bm, fm, lm)
		must(t, err)
		blockID := file.NewBlockID("testfile", 1)
		_, err = tx1.Pin(blockID)
		must(t, err)
		must(t, tx1.SetInt32(blockID, 80, 1, false))
		must(t, tx1.SetStr(blockID, 40, "one", false))
		must(t, tx1.Commit())

		tx2, err := NewTransaction(bm, fm, lm)
		must(t, err)
		_, err = tx2.Pin(blockID)
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

		tx3, err := NewTransaction(bm, fm, lm)
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

		tx4, err := NewTransaction(bm, fm, lm)
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
		slog.SetLogLoggerLevel(slog.LevelDebug)
		fm, err := file.NewManager(t.TempDir(), 128)
		if err != nil {
			t.Fatal(err)
		}
		lm, err := log.NewManager(fm, "logfile")
		if err != nil {
			t.Fatal(err)
		}
		bm := buffer.NewManager(fm, lm, 8)

		var g errgroup.Group
		g.Go(func() error {
			txA, err := NewTransaction(bm, fm, lm)
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
			txB, err := NewTransaction(bm, fm, lm)
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
			txC, err := NewTransaction(bm, fm, lm)
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
		slog.SetLogLoggerLevel(slog.LevelDebug)
		fm, err := file.NewManager(t.TempDir(), 128)
		if err != nil {
			t.Fatal(err)
		}
		lm, err := log.NewManager(fm, "logfile")
		if err != nil {
			t.Fatal(err)
		}
		bm := buffer.NewManager(fm, lm, 8)

		tx, err := NewTransaction(bm, fm, lm)
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
