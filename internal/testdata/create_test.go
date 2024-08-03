package testdata_test

import (
	"fmt"
	"os"
	"testing"

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
	w("CREATE TABLE courses(course_id INT, title VARCHAR(20), department_id INT);")
	w("CREATE TABLE sections(section_id INT, course_id INT, professor VARCHAR(8), year_offered int)")

	w, c = writer("create_indexes.sql")
	defer c()
	w("CREATE INDEX idx_departments_department_id ON departments (department_id);")
	w("CREATE INDEX idx_students_student_id ON students (student_id);")
	w("CREATE INDEX idx_courses_course_id ON courses (course_id);")
	w("CREATE INDEX idx_sections_section_id ON sections (section_id);")

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
			w(fmt.Sprintf("INSERT INTO courses (course_id, title, department_id) VALUES (%d, '%s', %d);", courseOffset+courseCount, sectionTitles[j], departmentOffset+i))
			courseCount++
		}
	}

	w("-- sections")
	const (
		sectionOffset = 400000
	)
	sectionCount := 1
	for i := 1; i <= courseCount; i++ {
		w(fmt.Sprintf("INSERT INTO sections (section_id, course_id, professor, year_offered) VALUES (%d, %d, '%s', %d);", sectionOffset+sectionCount, courseOffset+i, faker.FirstName(), faker.Year()))
		sectionCount++
	}
}
