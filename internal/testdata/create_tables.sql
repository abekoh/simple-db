CREATE TABLE students (student_id INT, name VARCHAR(10), major_department_id INT, grad_year INT);
CREATE TABLE departments (department_id INT, department_name VARCHAR(10));
CREATE TABLE courses(course_id int, title varchar(20), department_id int);
CREATE TABLE sections(section_id int, course_id int, professor varchar(8), year_offered int)
