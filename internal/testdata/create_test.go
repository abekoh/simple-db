package testdata_test

import (
	"os"
	"testing"
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
	w("CREATE TABLE students (student_id INT, name VARCHAR(10), major_department_id INT, grad_year INT);")
	w("CREATE TABLE departments (department_id INT, department_name VARCHAR(10));")
	w("CREATE TABLE courses(course_id int, title varchar(20), department_id int);")
	w("CREATE TABLE sections(section_id int, course_id int, professor varchar(8), year_offered int)")

	w, c = writer("create_indexes.sql")
	defer c()
	w("CREATE INDEX idx_students_student_id ON students (student_id);")
	w("CREATE INDEX idx_departments_department_id ON departments (department_id);")
	w("CREATE INDEX idx_courses_course_id ON courses (course_id);")
	w("CREATE INDEX idx_sections_section_id ON sections (section_id);")

	w, c = writer("insert_data.sql")
	defer c()
}
