package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
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
		address = "127.0.0.1:45432"
	}
	listen, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("could not listen on %s: %w", address, err)
	}
	slog.InfoContext(ctx, "Listening", "addr", listen.Addr())
	go func() {
		<-ctx.Done()
		listen.Close()
	}()

	dir := cfg.Dir
	if dir == "" {
		dir, err = os.MkdirTemp(os.TempDir(), "simpledb")
		if err != nil {
			return fmt.Errorf("could not create temp dir: %w", err)
		}
	}

	db, err := simpledb.New(ctx, dir)
	if err != nil {
		return fmt.Errorf("could not create SimpleDB: %w", err)
	}

	for {
		conn, err := listen.Accept()
		if err != nil {
			return fmt.Errorf("could not accept connection: %w", err)
		}
		slog.InfoContext(ctx, "Accepted connection", "remote_addr", conn.RemoteAddr())

		b := NewBackend(db, conn)

		go func() {
			err := b.Run()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					panic(err)
				}
			}
			slog.InfoContext(ctx, "Closed connection", "remote_addr", conn.RemoteAddr())
		}()
	}
}
