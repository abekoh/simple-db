package postgres

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgres(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := Config{
		Dir:     t.TempDir(),
		Address: "127.0.0.1:54329",
	}
	go func() {
		if err := RunServer(ctx, cfg); err != nil {
			t.Error(err)
		}
	}()

	conn, err := pgx.Connect(ctx, "postgres://127.0.0.1:54329/postgres")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn
}
