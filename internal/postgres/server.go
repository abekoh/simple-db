package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/abekoh/simple-db/internal/simpledb"
)

type Config struct {
	Dir     string
	Address string
}

func RunServer(ctx context.Context, cfg Config) error {
	address := cfg.Address
	if address == "" {
		address = "127.0.0.1:5432"
	}
	listen, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("could not listen on %s: %w", address, err)
	}
	slog.InfoContext(ctx, "Listening on %s", listen.Addr())

	for {
		conn, err := listen.Accept()
		if err != nil {
			return fmt.Errorf("could not accept connection: %w", err)
		}
		slog.InfoContext(ctx, "Accepted connection from %s", conn.RemoteAddr())

		dir := cfg.Dir
		if dir == "" {
			dir, err = os.MkdirTemp(os.TempDir(), "simpledb")
			if err != nil {
				fmt.Errorf("could not create temp dir: %w", err)
			}
		}

		db, err := simpledb.New(ctx, dir)
		if err != nil {
			return fmt.Errorf("could not create SimpleDB: %w", err)
		}

		b := NewBackend(db, conn)

		go func() {
			err := b.Run()
			if err != nil {
				fmt.Errorf("error running backend: %w", err)
			}
			slog.InfoContext(ctx, "Closed connection from %s", conn.RemoteAddr())
		}()
	}
}
