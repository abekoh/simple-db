CREATE TABLE departments (department_id INT, department_name VARCHAR(20));
CREATE TABLE students (student_id INT, name VARCHAR(10), major_id INT, grad_year INT);

INSERT INTO students (student_id, name, major_id, grad_year) VALUES (1, 'Alice', 1, 2018);
INSERT INTO students (student_id, name, major_id, grad_year) VALUES (2, 'Bob', 1, 2020);

SELECT student_id, name FROM students;
SELECT student_id, name FROM students WHERE student_id = 1;

UPDATE students SET name = 'Adam' WHERE student_id = 1;
SELECT student_id, name FROM students;

DELETE FROM students WHERE student_id = 1;
SELECT student_id, name FROM students;

BEGIN;
UPDATE students SET name = 'BOB' WHERE student_id = 2;
SELECT student_id, name FROM students;
ROLLBACK;
SELECT student_id, name FROM students;


INSERT INTO departments (department_id, department_name) VALUES (1, 'Computer Science');
INSERT INTO departments (department_id, department_name) VALUES (2, 'Mathematics');
INSERT INTO students (student_id, name, major_id, grad_year) VALUES (1, 'Alice', 1, 2018);
INSERT INTO students (student_id, name, major_id, grad_year) VALUES (2, 'Bob', 1, 2020);
INSERT INTO students (student_id, name, major_id, grad_year) VALUES (3, 'Charlie', 1, 2007);
INSERT INTO students (student_id, name, major_id, grad_year) VALUES (4, 'David', 2, 2019);
INSERT INTO students (student_id, name, major_id, grad_year) VALUES (5, 'Eve', 2, 1999);

SELECT name, department_name FROM students JOIN departments ON major_id = department_id;

EXPLAIN SELECT department_name FROM departments WHERE department_id = 10;
CREATE INDEX departments_pkey ON departments (department_id);
EXPLAIN SELECT department_name FROM departments WHERE department_id = 10;
EXPLAIN SELECT department_name, MIN(grad_year) AS min_grad_year FROM students JOIN departments ON major_id = department_id GROUP BY department_name ORDER BY department_name;
