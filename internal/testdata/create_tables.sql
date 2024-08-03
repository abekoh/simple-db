CREATE TABLE departments (department_id INT, department_name VARCHAR(10));
CREATE TABLE students (student_id INT, name VARCHAR(10), major_id INT, grad_year INT);
CREATE TABLE courses(course_id INT, title VARCHAR(20), department_id INT);
CREATE TABLE sections(section_id INT, course_id INT, professor VARCHAR(8), year_offered int)
