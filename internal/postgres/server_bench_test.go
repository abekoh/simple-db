package postgres

import (
	"context"
	"log/slog"
	"testing"

	"github.com/abekoh/simple-db/internal/testdata"
	"github.com/abekoh/simple-db/internal/transaction"
	"github.com/jackc/pgx/v5"
)

func BenchmarkPostgres_SelectOneRow(b *testing.B) {
	run := func(b *testing.B, srcDirname string) {
		b.Helper()

		slog.SetLogLoggerLevel(slog.LevelError)

		transaction.CleanupLockTable(b)
		ctx, cancel := context.WithCancel(context.Background())
		b.Cleanup(cancel)
		dir := b.TempDir()
		if err := testdata.CopySnapshotData(srcDirname, dir); err != nil {
			b.Fatal(err)
		}
		cfg := Config{
			Dir:     dir,
			Address: "127.0.0.1:54329",
		}
		go func() {
			_ = RunServer(ctx, cfg)
		}()

		pgCfg, err := pgx.ParseConfig("postgres://postgres@127.0.0.1:54329/postgres")
		if err != nil {
			b.Fatal(err)
		}
		conn, err := pgx.ConnectConfig(ctx, pgCfg)
		if err != nil {
			b.Fatal(err)
		}

		studentIDs := []int{
			200001,
			200376,
			204199,
			208321,
			210000,
		}

		type Row struct {
			StudentID int
			Name      string
		}

		b.ResetTimer()
		for _, studentID := range studentIDs {
			var r Row
			if err := conn.QueryRow(ctx, "SELECT student_id, name FROM students WHERE student_id = $1", studentID).Scan(&r.StudentID, &r.Name); err != nil {
				b.Fatal(err)
			}
			if r.StudentID != studentID {
				b.Errorf("unexpected student_id: %d", r.StudentID)
			}
		}
	}
	b.Run("no index", func(b *testing.B) {
		run(b, "tables_data")
	})
	b.Run("use index", func(b *testing.B) {
		run(b, "tables_indexes_data")
	})
}
