package postgres

import (
	"context"
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/transaction"
	"github.com/jackc/pgx/v5"
)

func TestPostgres(t *testing.T) {
	transaction.CleanupLockTable(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := Config{
		Dir:     t.TempDir(),
		Address: "127.0.0.1:54329",
	}
	go func() {
		_ = RunServer(ctx, cfg)
	}()

	pgCfg, err := pgx.ParseConfig("postgres://postgres@127.0.0.1:54329/postgres")
	if err != nil {
		t.Fatal(err)
	}
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

	for _, args := range [][]any{
		{1, "foo"},
		{2, "bar"},
		{3, "baz"},
	} {
		tag, err = conn.Exec(ctx, "INSERT INTO mytable (id, name) VALUES ($1, $2)", args...)
		if err != nil {
			t.Fatal(err)
		}
		if tag.String() != "INSERT 0 1" {
			t.Errorf("unexpected tag: %s", tag)
		}
	}

	tag, err = conn.Exec(ctx, "UPDATE mytable SET name = 'HOGE' WHERE id = $1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "UPDATE 1" {
		t.Errorf("unexpected tag: %s", tag)
	}

	tag, err = conn.Exec(ctx, "DELETE FROM mytable WHERE id = $1", 2)
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

	var queryRow Row
	if err := conn.QueryRow(ctx, "SELECT id, name FROM mytable WHERE id = $1", 3).Scan(&queryRow.ID, &queryRow.Name); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(queryRow, Row{ID: 3, Name: "HOGE"}) {
		t.Errorf("unexpected row: %v", queryRow)
	}
}

func TestPostgres_Transaction(t *testing.T) {
	transaction.CleanupLockTable(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := Config{
		Dir:     t.TempDir(),
		Address: "127.0.0.1:54329",
	}
	go func() {
		_ = RunServer(ctx, cfg)
	}()

	pgCfg, err := pgx.ParseConfig("postgres://postgres@127.0.0.1:54329/postgres")
	if err != nil {
		t.Fatal(err)
	}
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

	type Row struct {
		ID   int32
		Name string
	}

	assertID1 := func(expected Row) {
		var queryRow Row
		if err := conn.QueryRow(ctx, "SELECT id, name FROM mytable WHERE id = $1", 1).Scan(&queryRow.ID, &queryRow.Name); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(queryRow, expected) {
			t.Errorf("unexpected row: %v", queryRow)
		}
	}

	tag, err = conn.Exec(ctx, "INSERT INTO mytable (id, name) VALUES ($1, $2)", 1, "foo")
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "INSERT 0 1" {
		t.Errorf("unexpected tag: %s", tag)
	}
	assertID1(Row{ID: 1, Name: "foo"})

	conn1, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		t.Fatal(err)
	}
	tx1, err := conn1.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tag, err = tx1.Exec(ctx, "UPDATE mytable SET name = 'HOGE' WHERE id = $1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "UPDATE 1" {
		t.Errorf("unexpected tag: %s", tag)
	}
	err = tx1.Rollback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertID1(Row{ID: 1, Name: "foo"})

	conn2, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := conn2.Begin(ctx)
	tag, err = tx2.Exec(ctx, "UPDATE mytable SET name = 'HOGE' WHERE id = $1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if tag.String() != "UPDATE 1" {
		t.Errorf("unexpected tag: %s", tag)
	}
	err = tx2.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertID1(Row{ID: 1, Name: "HOGE"})
}
