package postgres

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/statement"
	"github.com/abekoh/simple-db/internal/transaction"
	"github.com/jackc/pgx/v5"
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

	if err := b.handleStartup(); err != nil {
		return fmt.Errorf("error handling startup: %w", err)
	}

	var (
		bound    statement.Bound
		tx       *transaction.Transaction
		queryErr error
	)
	for {
		var buf []byte
		readyForQuery := false

		msg, err := b.backend.Receive()
		if err != nil {
			queryErr = fmt.Errorf("error receiving message: %w", err)
		} else {
			switch m := msg.(type) {
			case *pgproto3.Query:
				buf, tx, err = b.handleQuery(buf, m.String, tx)
				if err != nil {
					queryErr = fmt.Errorf("error handling query: %w", err)
					break
				}
				readyForQuery = true
			case *pgproto3.Parse:
				if len(m.Name) == 0 {
					queryErr = fmt.Errorf("empty statement name")
					break
				}
				tx, err := b.db.NewTx(context.Background())
				if err != nil {
					queryErr = fmt.Errorf("error creating new transaction: %w", err)
					break
				}
				prepared, err := b.db.Planner().Prepare(m.Query, tx)
				if err != nil {
					queryErr = fmt.Errorf("error preparing statement: %w", err)
					break
				}
				b.db.StmtMgr().Add(m.Name, prepared)
				buf, queryErr = (&pgproto3.ParseComplete{}).Encode(buf)
				if err != nil {
					queryErr = fmt.Errorf("error encoding parse complete: %w", err)
					break
				}
			case *pgproto3.Bind:
				if len(m.PreparedStatement) == 0 {
					queryErr = fmt.Errorf("empty prepared statement name")
					break
				}
				prepared, err := b.db.StmtMgr().Get(m.PreparedStatement)
				if err != nil {
					queryErr = fmt.Errorf("error getting statement: %w", err)
					break
				}
				params := make(map[int]schema.Constant)
				for i, v := range m.Parameters {
					format := m.ParameterFormatCodes[i]
					switch format {
					case pgx.TextFormatCode:
						params[i+1] = schema.ConstantStr(v)
					case pgx.BinaryFormatCode:
						if len(v) != 4 {
							queryErr = fmt.Errorf("invalid binary parameter length: %d", len(v))
							break
						}
						n := int32(v[0])<<24 | int32(v[1])<<16 | int32(v[2])<<8 | int32(v[3])
						params[i+1] = schema.ConstantInt32(n)
					}
				}
				if err != nil {
					break
				}
				bound, err = prepared.SwapParams(params)
				if err != nil {
					queryErr = fmt.Errorf("error swapping params: %w", err)
					break
				}
				buf, err = (&pgproto3.BindComplete{}).Encode(buf)
				if err != nil {
					queryErr = fmt.Errorf("error encoding bind complete: %w", err)
					break
				}
			case *pgproto3.Describe:
				if len(m.Name) == 0 {
					buf, err = (&pgproto3.NoData{}).Encode(buf)
					if err != nil {
						queryErr = fmt.Errorf("error encoding no data: %w", err)
						break
					}
					continue
				}
				prepared, err := b.db.StmtMgr().Get(m.Name)
				if err != nil {
					queryErr = fmt.Errorf("error getting statement: %w", err)
					break
				}
				tx, err := b.db.NewTx(context.Background())
				if err != nil {
					queryErr = fmt.Errorf("error creating new transaction: %w", err)
					break
				}
				placeholders := prepared.Placeholders(func(tableName string) (*schema.Schema, error) {
					l, err := b.db.MetadataMgr().Layout(tableName, tx)
					if err != nil {
						return nil, fmt.Errorf("layout error: %w", err)
					}
					return l.Schema(), nil
				})
				paramOIDs := make([]uint32, len(placeholders))
				for i, fieldType := range placeholders {
					switch fieldType {
					case schema.Integer32:
						paramOIDs[i-1] = pgtype.Int4OID
					case schema.Varchar:
						paramOIDs[i-1] = pgtype.TextOID
					}
				}
				buf, err = (&pgproto3.ParameterDescription{
					ParameterOIDs: paramOIDs,
				}).Encode(buf)
				if err != nil {
					queryErr = fmt.Errorf("error encoding parameter description: %w", err)
					break
				}
			case *pgproto3.Sync:
				buf, err = (&pgproto3.NoData{}).Encode(buf)
				if err != nil {
					queryErr = fmt.Errorf("error encoding no data: %w", err)
					break
				}
				readyForQuery = true
			case *pgproto3.Execute:
				if bound == nil {
					queryErr = fmt.Errorf("no query to execute")
					break
				}
				buf, tx, err = b.handleBound(buf, bound, tx)
				if err != nil {
					queryErr = fmt.Errorf("error handling query: %w", err)
					break
				}
				bound = nil
			case *pgproto3.Terminate:
			default:
				queryErr = fmt.Errorf("received not supported message: %#v", m)
			}
		}

		if queryErr != nil {
			buf, err = (&pgproto3.ErrorResponse{Message: queryErr.Error()}).Encode(nil)
			if err != nil {
				return fmt.Errorf("error encoding error response: %w", err)
			}
			readyForQuery = true
		}
		if readyForQuery {
			buf, err = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			if err != nil {
				return fmt.Errorf("error encoding ready for query: %w", err)
			}
		}
		_, err = b.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error writing parse complete: %w", err)
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

func (b *Backend) handleQuery(buf []byte, query string, tx *transaction.Transaction) ([]byte, *transaction.Transaction, error) {
	return b.execute(buf, func(t *transaction.Transaction) (plan.Result, error) {
		return b.db.Planner().Execute(query, t)
	}, tx)
}

func (b *Backend) handleBound(buf []byte, bound statement.Bound, tx *transaction.Transaction) ([]byte, *transaction.Transaction, error) {
	return b.execute(buf, func(t *transaction.Transaction) (plan.Result, error) {
		return b.db.Planner().ExecuteBound(bound, t)
	}, tx)
}

func (b *Backend) execute(buf []byte, exec func(t *transaction.Transaction) (plan.Result, error), tx *transaction.Transaction) ([]byte, *transaction.Transaction, error) {
	oneQueryTx := tx == nil

	if tx == nil {
		ctx := context.Background()
		x, err := b.db.NewTx(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating new transaction: %w", err)
		}
		tx = x
	}

	defer func() {
		if oneQueryTx && tx != nil {
			tx.Rollback()
			tx = nil
		}
	}()

	res, err := exec(tx)
	if err != nil {
		return nil, nil, fmt.Errorf("error executing query: %w", err)
	}

	switch r := res.(type) {
	case plan.Plan:
		if bp, ok := r.(*plan.BoundPlan); ok {
			r = bp.Plan
		}

		if _, ok := r.(*plan.ExplainPlan); ok {
			buf, err = (&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("QUERY PLAN"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          pgtype.TextOID,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			}}).Encode(buf)
			if err != nil {
				return nil, nil, fmt.Errorf("error encoding row description: %w", err)
			}
			infoText := r.Info().String()
			buf, err = (&pgproto3.DataRow{Values: [][]byte{[]byte(infoText)}}).Encode(buf)
			if err != nil {
				return nil, nil, fmt.Errorf("error encoding data row: %w", err)
			}
			buf, err = (&pgproto3.CommandComplete{CommandTag: []byte("EXPLAIN")}).Encode(buf)
			if err != nil {
				return nil, nil, fmt.Errorf("error encoding command complete: %w", err)
			}
		} else {
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
					return nil, nil, fmt.Errorf("unsupported field type: %v", sche.Typ(fieldName))
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
				return nil, nil, fmt.Errorf("error encoding row description: %w", err)
			}

			scan, err := r.Open()
			if err != nil {
				return nil, nil, fmt.Errorf("error opening plan: %w", err)
			}
			defer scan.Close()
			count := 0
			for {
				ok, err := scan.Next()
				if err != nil {
					return nil, nil, fmt.Errorf("error scanning: %w", err)
				}
				if !ok {
					break
				}
				values := make([][]byte, len(fieldNames))
				for i, fieldName := range fieldNames {
					val, err := scan.Val(fieldName)
					if err != nil {
						return nil, nil, fmt.Errorf("error getting value: %w", err)
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
					return nil, nil, fmt.Errorf("error encoding data row: %w", err)
				}
				count++
			}
			buf, err = (&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", count))}).Encode(buf)
			if err != nil {
				return nil, nil, fmt.Errorf("error encoding command complete: %w", err)
			}
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
		case plan.Begin:
			commandTag = []byte("BEGIN")
			oneQueryTx = false
		case plan.Commit:
			commandTag = []byte("COMMIT")
			if err := tx.Commit(); err != nil {
				return nil, nil, fmt.Errorf("error committing transaction: %w", err)
			}
			tx = nil
			oneQueryTx = false
		case plan.Rollback:
			commandTag = []byte("ROLLBACK")
			if err := tx.Rollback(); err != nil {
				return nil, nil, fmt.Errorf("error rolling back transaction: %w", err)
			}
			tx = nil
			oneQueryTx = false
		}
		buf, err = (&pgproto3.CommandComplete{CommandTag: commandTag}).Encode(buf)
		if err != nil {
			return nil, nil, fmt.Errorf("error encoding command complete: %w", err)
		}
	default:
		return nil, nil, fmt.Errorf("unknown result type: %#v", res)
	}

	if oneQueryTx {
		if err := tx.Commit(); err != nil {
			return nil, nil, fmt.Errorf("error committing transaction: %w", err)
		}
		tx = nil
	}

	return buf, tx, nil
}
