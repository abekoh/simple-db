package postgres

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
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

		switch m := msg.(type) {
		case *pgproto3.Query:
			buf, err := b.handleQuery(m.String)
			if err != nil {
				return fmt.Errorf("error handling query: %w", err)
			}
			_, err = b.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Parse:
			if len(m.Name) == 0 {
				return fmt.Errorf("empty statement name")
			}
			b.db.StmtMgr().Add(m.Name, m.Query)
			buf, err := (&pgproto3.ParseComplete{}).Encode(nil)
			if err != nil {
				return fmt.Errorf("error encoding parse complete: %w", err)
			}
			buf, err = (&pgproto3.NoData{}).Encode(buf)
			if err != nil {
				return fmt.Errorf("error encoding no data: %w", err)
			}
			buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			if err != nil {
				return fmt.Errorf("error encoding ready for query: %w", err)
			}
			_, err = b.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing parse complete: %w", err)
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

func (b *Backend) handleQuery(query string) ([]byte, error) {
	ctx := context.Background()
	tx, err := b.db.NewTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating new transaction: %w", err)
	}
	res, err := b.db.Planner().Execute(query, tx)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer tx.Rollback()
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	var buf []byte
	switch r := res.(type) {
	case plan.Plan:
		sche := r.Schema()
		fieldNames := sche.FieldNames()
		fields := make([]pgproto3.FieldDescription, len(fieldNames))
		for i, fieldName := range fieldNames {
			var dataTypeOID uint32
			var dataTypeSize int16
			switch sche.Typ(fieldName) {
			case schema.Integer32:
				dataTypeOID = pgtype.Int4OID
				dataTypeSize = 4
			case schema.Varchar:
				dataTypeOID = pgtype.TextOID
				dataTypeSize = -1
			default:
				return nil, fmt.Errorf("unsupported field type: %v", sche.Typ(fieldName))
			}
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(fieldName),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          dataTypeOID,
				DataTypeSize:         dataTypeSize,
				TypeModifier:         -1,
				Format:               0,
			}
		}
		buf, err = (&pgproto3.RowDescription{Fields: fields}).Encode(buf)
		if err != nil {
			return nil, fmt.Errorf("error encoding row description: %w", err)
		}

		scan, err := r.Open()
		if err != nil {
			return nil, fmt.Errorf("error opening plan: %w", err)
		}
		defer scan.Close()
		count := 0
		for {
			ok, err := scan.Next()
			if err != nil {
				return nil, fmt.Errorf("error scanning: %w", err)
			}
			if !ok {
				break
			}
			values := make([][]byte, len(fieldNames))
			for i, fieldName := range fieldNames {
				val, err := scan.Val(fieldName)
				if err != nil {
					return nil, fmt.Errorf("error getting value: %w", err)
				}
				var row []byte
				switch v := val.(type) {
				case schema.ConstantInt32:
					row = []byte(strconv.Itoa(int(v)))
				case schema.ConstantStr:
					row = []byte(v)
				}
				values[i] = row
			}
			buf, err = (&pgproto3.DataRow{Values: values}).Encode(buf)
			if err != nil {
				return nil, fmt.Errorf("error encoding data row: %w", err)
			}
			count++
		}
		buf, err = (&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", count))}).Encode(buf)
		if err != nil {
			return nil, fmt.Errorf("error encoding command complete: %w", err)
		}
	case plan.CommandResult:
		var commandTag []byte
		switch r.Type {
		case plan.Insert:
			commandTag = []byte(fmt.Sprintf("INSERT 0 %d", r.Count))
		case plan.Delete:
			commandTag = []byte(fmt.Sprintf("DELETE %d", r.Count))
		case plan.Update:
			commandTag = []byte(fmt.Sprintf("UPDATE %d", r.Count))
		case plan.CreateTable:
			commandTag = []byte("CREATE TABLE")
		case plan.CreateView:
			commandTag = []byte(fmt.Sprintf("SELECT %d", r.Count))
		case plan.CreateIndex:
			commandTag = []byte(fmt.Sprintf("SELECT %d", r.Count))
		}
		buf, err = (&pgproto3.CommandComplete{CommandTag: commandTag}).Encode(buf)
		if err != nil {
			return nil, fmt.Errorf("error encoding command complete: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown result type: %#v", res)
	}
	buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
	if err != nil {
		return nil, fmt.Errorf("error encoding ready for query: %w", err)
	}
	return buf, nil
}
