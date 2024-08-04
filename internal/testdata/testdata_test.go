package testdata_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/abekoh/simple-db/internal/simpledb"
	"github.com/abekoh/simple-db/internal/testdata"
	"github.com/abekoh/simple-db/internal/transaction"
	"github.com/brianvoe/gofakeit/v7"
)

func TestCreateTestdata(t *testing.T) {
	writer := func(filename string) (write func(string), close func()) {
		f, err := os.Create(filename)
		if err != nil {
			t.Fatal(err)
		}
		write = func(s string) {
			_, err = f.WriteString(s + "\n")
			if err != nil {
				t.Fatal(err)
			}
		}
		close = func() {
			err = f.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		return
	}

	w, c := writer("create_tables.sql")
	defer c()
	w("CREATE TABLE departments (department_id INT, department_name VARCHAR(10));")
	w("CREATE TABLE students (student_id INT, name VARCHAR(10), major_id INT, grad_year INT);")
	w("CREATE TABLE courses(course_id INT, title VARCHAR(20), course_department_id INT);")
	w("CREATE TABLE sections(section_id INT, section_course_id INT, professor VARCHAR(8), year_offered int)")

	w, c = writer("create_indexes.sql")
	defer c()
	w("CREATE INDEX departments_pkey ON departments (department_id);")
	w("CREATE INDEX students_pkey ON students (student_id);")
	w("CREATE INDEX courses_pkey ON courses (course_id);")
	w("CREATE INDEX sections_pkey ON sections (section_id);")
	w("CREATE INDEX students_major_department_id ON students (major_department_id);")
	w("CREATE INDEX courses_course_department_id ON courses (course_department_id);")
	w("CREATE INDEX sections_section_course_id ON sections (section_course_id);")

	faker := gofakeit.New(523207)

	w, c = writer("insert_data.sql")
	defer c()
	w("-- departments")
	const (
		departmentOffset = 100000
		departmentLength = 100
	)
	for i := 1; i <= departmentLength; i++ {
		w(fmt.Sprintf("INSERT INTO departments (department_id, department_name) VALUES (%d, '%s');", departmentOffset+i, faker.Language()))
	}

	w("-- students")
	const (
		studentOffset = 200000
		studentLength = 10000
	)
	for i := 1; i <= studentLength; i++ {
		w(fmt.Sprintf("INSERT INTO students (student_id, name, major_id, grad_year) VALUES (%d, '%s', %d, %d);", studentOffset+i, faker.FirstName(), faker.Number(departmentOffset, departmentOffset+departmentLength-1), faker.Year()))
	}

	w("-- courses")
	const (
		courseOffset = 300000
	)
	sectionTitles := []string{"Intro 1", "Intro 2", "Intro 3", "Advanced 1", "Advanced 2"}
	courseCount := 1
	for i := 1; i <= departmentLength; i++ {
		sectionN := faker.Number(0, len(sectionTitles))
		for j := 0; j < sectionN; j++ {
			w(fmt.Sprintf("INSERT INTO courses (course_id, title, course_department_id) VALUES (%d, '%s', %d);", courseOffset+courseCount, sectionTitles[j], departmentOffset+i))
			courseCount++
		}
	}

	w("-- sections")
	const (
		sectionOffset = 400000
	)
	sectionCount := 1
	for i := 1; i <= courseCount; i++ {
		w(fmt.Sprintf("INSERT INTO sections (section_id, section_course_id, professor, year_offered) VALUES (%d, %d, '%s', %d);", sectionOffset+sectionCount, courseOffset+i, faker.FirstName(), faker.Year()))
		sectionCount++
	}
}

func TestCreateSnapshots(t *testing.T) {
	createSnapshot := func(t *testing.T, dirname string, sqlFilenames ...string) {
		t.Helper()

		err := os.RemoveAll(dirname)
		if err != nil {
			t.Fatal(err)
		}
		err = os.MkdirAll(dirname, 0755)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		db, err := simpledb.New(ctx, dirname)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.NewTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sqlIter := testdata.SQLIterator(sqlFilenames...)
		count := 0
		for sql, err := range sqlIter {
			t.Logf("execute %s", sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err := db.Planner().Execute(sql, tx)
			if err != nil {
				t.Fatal(err)
			}
			if count++; count%100 == 0 {
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				tx, err = db.NewTx(ctx)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("snapshots/tables", func(t *testing.T) {
		t.Skip()
		transaction.CleanupLockTable(t)
		createSnapshot(t, "snapshots/tables", "create_tables.sql")
	})
	t.Run("snapshots/tables_data", func(t *testing.T) {
		t.Skip()
		transaction.CleanupLockTable(t)
		createSnapshot(t, "snapshots/tables_data", "create_tables.sql", "insert_data.sql")
	})
	t.Run("snapshots/tables_indexes_data", func(t *testing.T) {
		t.Skip()
		transaction.CleanupLockTable(t)
		createSnapshot(t, "snapshots/tables_indexes_data", "create_tables.sql", "create_indexes.sql", "insert_data.sql")
	})
}
