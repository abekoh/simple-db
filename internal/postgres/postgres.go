package postgres

import (
	"context"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Backend struct {
	backend *pgproto3.Backend
	conn    net.Conn
}

func NewBackend(conn net.Conn) *Backend {
	return &Backend{
		backend: pgproto3.NewBackend(conn, conn),
		conn:    conn,
	}
}

func (b *Backend) Run(ctx context.Context) error {
	defer b.Close()
	return nil
}

func (b *Backend) Close() error {
	return b.conn.Close()
}
