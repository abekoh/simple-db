package postgres

import (
	"fmt"
	"net"

	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Backend struct {
	db      *simpledb.DB
	backend *pgproto3.Backend
	conn    net.Conn
}

func NewBackend(db *simpledb.DB, conn net.Conn) *Backend {
	return &Backend{
		db:      db,
		backend: pgproto3.NewBackend(conn, conn),
		conn:    conn,
	}
}

func (b *Backend) Run() error {
	defer b.Close()

	err := b.handleStartup()
	if err != nil {
		return fmt.Errorf("error handling startup: %w", err)
	}

	for {
		msg, err := b.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg.(type) {
		case *pgproto3.Query:
			buf, err := (&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("fortune"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			}}).Encode(nil)
			if err != nil {
				return fmt.Errorf("error encoding row description: %w", err)
			}
			buf, err = (&pgproto3.DataRow{Values: [][]byte{[]byte("a")}}).Encode(buf)
			if err != nil {
				return fmt.Errorf("error encoding data row: %w", err)
			}
			buf, err = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
			if err != nil {
				return fmt.Errorf("error encoding command complete: %w", err)
			}
			buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			if err != nil {
				return fmt.Errorf("error encoding ready for query: %w", err)
			}
			_, err = b.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Terminate:
			return nil
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

func (b *Backend) Close() error {
	return b.conn.Close()
}

func (b *Backend) handleStartup() error {
	startupMessage, err := b.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf, err := (&pgproto3.AuthenticationOk{}).Encode(nil)
		if err != nil {
			return fmt.Errorf("error encoding authentication ok: %w", err)
		}
		buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		if err != nil {
			return fmt.Errorf("error encoding ready for query: %w", err)
		}
		_, err = b.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = b.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return b.handleStartup()
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}
