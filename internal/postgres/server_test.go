package postgres

import (
	"context"
	"reflect"
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

	pgCfg, err := pgx.ParseConfig("postgres://127.0.0.1:54329/postgres")
	if err != nil {
		t.Fatal(err)
	}
	pgCfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	conn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		t.Fatal(err)
	}

	tag, err := conn.Exec(ctx, "CREATE TABLE mytable (id INT, name VARCHAR(10))")
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "CREATE TABLE" {
		t.Errorf("unexpected tag: %s", tag)
	}

	for _, query := range []string{
		"INSERT INTO mytable (id, name) VALUES (1, 'foo')",
		"INSERT INTO mytable (id, name) VALUES (2, 'bar')",
		"INSERT INTO mytable (id, name) VALUES (3, 'baz')",
	} {
		tag, err = conn.Exec(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		if tag.String() != "INSERT 0 1" {
			t.Errorf("unexpected tag: %s", tag)
		}
	}

	tag, err = conn.Exec(ctx, "UPDATE mytable SET name = 'HOGE' WHERE id = 3")
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "UPDATE 1" {
		t.Errorf("unexpected tag: %s", tag)
	}

	tag, err = conn.Exec(ctx, "DELETE FROM mytable WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "DELETE 1" {
		t.Errorf("unexpected tag: %s", tag)
	}

	rows, err := conn.Query(ctx, "SELECT id, name FROM mytable")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type Row struct {
		ID   int32
		Name string
	}
	resRows := make([]Row, 0)
	for rows.Next() {
		var row Row
		if err := rows.Scan(&row.ID, &row.Name); err != nil {
			t.Fatal(err)
		}
		resRows = append(resRows, row)
	}
	if len(resRows) != 2 {
		t.Errorf("unexpected rows: %v", resRows)
	}
	if !reflect.DeepEqual(resRows, []Row{
		{ID: 1, Name: "foo"},
		{ID: 3, Name: "HOGE"},
	}) {
		t.Errorf("unexpected rows: %v", resRows)
	}

}
