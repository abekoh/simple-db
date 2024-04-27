package metadata

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/abekoh/simple-db/internal/buffer"
	"github.com/abekoh/simple-db/internal/file"
	"github.com/abekoh/simple-db/internal/log"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/transaction"
)

func TestMetadataManager(t *testing.T) {
	t.Parallel()

	fm, err := file.NewManager(t.TempDir(), 400)
	if err != nil {
		t.Fatal(err)
	}
	lm, err := log.NewManager(fm, "logfile")
	if err != nil {
		t.Fatal(err)
	}
	bm := buffer.NewManager(fm, lm, 8)

	tx, err := transaction.NewTransaction(bm, fm, lm)
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewManager(true, tx)
	if err != nil {
		t.Fatal(err)
	}

	schema := record.NewSchema()
	schema.AddInt32Field("A")
	schema.AddStrField("B", 9)
	if err := m.CreateTable("MyTable", schema, tx); err != nil {
		t.Fatal(err)
	}

	// TableManager
	layout, err := m.Layout("MyTable", tx)
	if err != nil {
		t.Fatal(err)
	}
	if layout.SlotSize() != 21 {
		t.Errorf("expected 21, got %d", layout.SlotSize())
	}
	var fields []string
	for _, fieldName := range layout.Schema().FieldNames() {
		switch layout.Schema().Typ(fieldName) {
		case record.Integer32:
			fields = append(fields, fmt.Sprintf("%s: int", fieldName))
		case record.Varchar:
			fields = append(fields, fmt.Sprintf("%s: varchar(%d)", fieldName, layout.Schema().Length(fieldName)))
		}
	}
	slices.Sort(fields)
	if !reflect.DeepEqual(fields, []string{"A: int", "B: varchar(9)"}) {
		t.Errorf("expected [A: int, B: varchar(9)], got %v", fields)
	}

	if tx.Commit() != nil {
		t.Fatal(err)
	}

	// StatManager
	scan, err := record.NewTableScan(tx, "MyTable", layout)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := scan.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := scan.SetInt32("A", int32(i)); err != nil {
			t.Fatal(err)
		}
		if err := scan.SetStr("B", fmt.Sprintf("rec%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	statInfo, err := m.StatInfo("MyTable", layout, tx)
	if err != nil {
		t.Fatal(err)
	}
	if statInfo.BlocksAccessed() != 3 {
		t.Errorf("expected 3, got %d", statInfo.BlocksAccessed())
	}
	if statInfo.RecordsOutput() != 50 {
		t.Errorf("expected 50, got %d", statInfo.RecordsOutput())
	}
	if statInfo.DistinctValues("A") != 17 {
		t.Errorf("expected 17, got %d", statInfo.DistinctValues("A"))
	}
	if statInfo.DistinctValues("B") != 17 {
		t.Errorf("expected 17, got %d", statInfo.DistinctValues("B"))
	}

	// ViewManager
	viewDef := "SELECT B FROM MyTable WHERE A = 1"
	if err := m.CreateView("MyView", viewDef, tx); err != nil {
		t.Fatal(err)
	}
	gotViewDef, err := m.ViewDef("MyView", tx)
	if err != nil {
		t.Fatal(err)
	}
	if gotViewDef != viewDef {
		t.Errorf("expected %s, got %s", "SELECT B FROM MyTable WHERE A = 1", gotViewDef)
	}

	// IndexManager
	if err := m.CreateIndex("IndexA", "MyTable", "A", tx); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateIndex("IndexB", "MyTable", "B", tx); err != nil {
		t.Fatal(err)
	}
	indexMap, err := m.IndexInfo("MyTable", tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexMap) != 2 {
		t.Errorf("expected 2, got %d", len(indexMap))
	}
	// TODO: assert indexMap
}
