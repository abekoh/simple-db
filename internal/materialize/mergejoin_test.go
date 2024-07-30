package materialize_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/abekoh/simple-db/internal/materialize"
	plan2 "github.com/abekoh/simple-db/internal/plan"
	"github.com/abekoh/simple-db/internal/record"
	"github.com/abekoh/simple-db/internal/record/schema"
	"github.com/abekoh/simple-db/internal/simpledb"
)

func TestMergeJoinPlan(t *testing.T) {
	ctx := context.Background()
	db, err := simpledb.New(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.NewTx(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sche1 := schema.NewSchema()
	sche1.AddInt32Field("department_id")
	sche1.AddStrField("department_name", 10)
	if err := db.MetadataMgr().CreateTable("departments", sche1, tx); err != nil {
		t.Fatal(err)
	}
	layout1 := record.NewLayoutSchema(sche1)
	updateScan1, err := record.NewTableScan(tx, "departments", layout1)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateScan1.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		departmentID   int32
		departmentName string
	}{
		{10, "compsci"},
		{18, "basketry"},
		{20, "math"},
		{30, "drama"},
	} {
		if err := updateScan1.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := updateScan1.SetInt32("department_id", v.departmentID); err != nil {
			t.Fatal(err)
		}
		if err := updateScan1.SetStr("department_name", v.departmentName); err != nil {
			t.Fatal(err)
		}
	}
	if err := updateScan1.Close(); err != nil {
		t.Fatal(err)
	}

	sche2 := schema.NewSchema()
	sche2.AddInt32Field("student_id")
	sche2.AddStrField("student_name", 10)
	sche2.AddInt32Field("department_id")
	if err := db.MetadataMgr().CreateTable("students", sche2, tx); err != nil {
		t.Fatal(err)
	}
	layout2 := record.NewLayoutSchema(sche2)
	updateScan2, err := record.NewTableScan(tx, "students", layout2)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateScan2.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		studentID    int32
		studentName  string
		departmentID int32
	}{
		{4, "sue", 20},
		{1, "joe", 10},
		{5, "bob", 30},
		{2, "amy", 20},
		{6, "kim", 20},
		{3, "max", 10},
		{8, "pat", 20},
		{7, "art", 30},
		{9, "lee", 10},
	} {
		if err := updateScan2.Insert(); err != nil {
			t.Fatal(err)
		}
		if err := updateScan2.SetInt32("student_id", v.studentID); err != nil {
			t.Fatal(err)
		}
		if err := updateScan2.SetStr("student_name", v.studentName); err != nil {
			t.Fatal(err)
		}
		if err := updateScan2.SetInt32("department_id", v.departmentID); err != nil {
			t.Fatal(err)
		}
	}
	if err := updateScan2.Close(); err != nil {
		t.Fatal(err)
	}

	tablePlan1, err := plan2.NewTablePlan("departments", tx, db.MetadataMgr())
	if err != nil {
		t.Fatal(err)
	}
	tablePlan2, err := plan2.NewTablePlan("students", tx, db.MetadataMgr())
	if err != nil {
		t.Fatal(err)
	}
	joinPlan, err := materialize.NewMergeJoinPlan(tx, tablePlan1, tablePlan2, "department_id", "department_id")
	if err != nil {
		t.Fatal(err)
	}
	projectPlan := plan2.NewProjectPlan(joinPlan, []schema.FieldName{"student_name", "department_name"})

	queryScan, err := projectPlan.Open()
	if err != nil {
		t.Fatal(err)
	}
	if err := queryScan.BeforeFirst(); err != nil {
		t.Fatal(err)
	}
	res := make([]string, 0, 9)
	for {
		ok, err := queryScan.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		studentName, err := queryScan.Str("student_name")
		if err != nil {
			t.Fatal(err)
		}
		departmentName, err := queryScan.Str("department_name")
		if err != nil {
			t.Fatal(err)
		}
		res = append(res, studentName+" "+departmentName)
	}
	if err := queryScan.Close(); err != nil {
		t.Fatal(err)
	}
	if len(res) != 9 {
		t.Fatalf("got %d, want 9", len(res))
	}
	if !reflect.DeepEqual(res, []string{
		"joe compsci",
		"max compsci",
		"amy math",
		"kim math",
		"pat math",
		"sue math",
		"art drama",
		"bob drama",
		"lee compsci",
	}) {
		t.Fatalf("unexpected result: %v", res)
	}
}
