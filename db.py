import psycopg2 as pg


class Database:
    def __init__(self, database, user, password,**kwargs):
        self.params = {
            'database': database,
            'user': user,
            'password': password,
        }
        self.create_db()

    def create_db(self):
        db_name = 'studentdb'
        with pg.connect(**self.params) as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS student(
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    gpa NUMERIC(10, 2),
                    birth TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS course(
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                );
                CREATE TABLE IF NOT EXISTS student_course(
                    student_id INT REFERENCES student(id),
                    course_id INT REFERENCES course(id),
                    PRIMARY KEY (student_id, course_id)
                );
            """, (db_name,))

    def get_students(self, course_name):
            with pg.connect(**self.params) as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT s.name
                    FROM student_course AS s_c
                    JOIN student AS s ON s_c.student_id = s.id
                    JOIN course AS c on s_c.course_id = c.id
                    WHERE c.name = %s;
                """, (course_name,))
                return [response[0] for response in cur.fetchall()]

    def add_students(self, course_name, students):
            with pg.connect(**self.params) as conn:
                cur = conn.cursor()
                cur.execute('SELECT id FROM course WHERE course.name = %s', (course_name,))
                response = cur.fetchone()
                if not response:
                    print('Такого курса нет')
                    return
                course_id = response[0]

                for student in students:
                    student_id = self.add_student(student)
                    cur.execute(
                        'INSERT INTO student_course (student_id, course_id) VALUES (%s, %s);',
                        (student_id, course_id)
                    )

    def add_student(self, student):
        with pg.connect(**self.params) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO student (name, gpa, birth) VALUES (%s, %s, %s)
                RETURNING id;
            """, (student.get('name'), student.get('gpa', 0), student.get('birth', '1980-01-01')))
            student_id = cur.fetchone()[0]
            print(f'Студент c id={student_id} создан')
            return student_id

    def get_student(self, student_id):
        with pg.connect(**self.params) as connection:
            cur = connection.cursor()
            cur.execute('SELECT * FROM Student WHERE id = %s;', (student_id,))
            return cur.fetchone()

    def add_course(self, course_name):
        with pg.connect(**self.params) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO course (name)
                VALUES (%s)
                RETURNING id;
            """, (course_name,))
            course_id = cur.fetchone()[0]
            print(f'Курс c id={course_id} создан')
            return course_id

if __name__ == '__main__':
    my_db = Database('netology', 'net1', 'net2')
    
    my_db.add_course('py')
    
    student = [
        {
            'name': 'Петр',
            'gpa': '4.8',
            'birth': '1982-10-03',
        }
    ]
    my_db.add_students('py', student)

    print(my_db.get_students('py'))