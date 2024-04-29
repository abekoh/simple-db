package query_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/server"
)

func TestScan(t *testing.T) {
	t.Run("Scan1", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		db, err := server.NewSimpleDB(ctx, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.NewTx(ctx)
		if err != nil {
			t.Fatal(err)
		}

		sche := schema.NewSchema()
		sche.AddInt32Field("A")
		sche.AddStrField("B", 9)
		layout := record.NewLayoutSchema(sche)
		scan1, err := record.NewTableScan(tx, "T", layout)
		if err != nil {
			t.Fatal(err)
		}

		if err := scan1.BeforeFirst(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 200; i++ {
			if err := scan1.Insert(); err != nil {
				t.Fatal(err)
			}
			if err := scan1.SetInt32("A", int32(i)); err != nil {
				t.Fatal(err)
			}
			if err := scan1.SetStr("B", fmt.Sprintf("rec%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := scan1.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
