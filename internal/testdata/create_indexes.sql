CREATE INDEX departments_pkey ON departments (department_id);
CREATE INDEX students_pkey ON students (student_id);
CREATE INDEX courses_pkey ON courses (course_id);
CREATE INDEX sections_pkey ON sections (section_id);
CREATE INDEX students_major_department_id ON students (major_department_id);
CREATE INDEX courses_course_department_id ON courses (course_department_id);
CREATE INDEX sections_section_course_id ON sections (section_course_id);
