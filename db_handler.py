import psycopg2 as pg


def create_db():
    with pg.connect(dbname='school_db', user='school_user') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                create table student (
                    id serial PRIMARY KEY,
                    name varchar(100) not null,
                    gpa numeric(10,2),
                    birth timestamp with time zone
                    );
                    """)
            cur.execute("""
                create table course (
                    id serial PRIMARY KEY,
                    name varchar(100) not null
                    );
                    """)
            cur.execute("""
                create table student_course (
                    id serial PRIMARY KEY,
                    student_id INTEGER REFERENCES student(id),
                    course_id INTEGER REFERENCES course(id)
                    );
                    """)


def add_course(*courses):
    sql_query_list = list()
    for course in courses:
        course_data = tuple([course])
        sql_query_list.append(course_data)
    with pg.connect(dbname='school_db', user='school_user') as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                insert into course (name) values (%s);
                    """, sql_query_list)


def add_students(course_id, *students):
    for student in students:
        add_student(student)
        with pg.connect(dbname='school_db', user='school_user') as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    select id from student where name = %s;
                        """, (student['name'],))
                id_value = cur.fetchall()[0][0]
                cur.execute("""
                    insert into student_course (student_id, course_id)
                    values (%s, %s);
                        """, (id_value, course_id))


def add_student(student):
    with pg.connect(dbname='school_db', user='school_user') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                insert into student (name, gpa, birth) values (%s, %s, %s);
                    """, (student['name'], student['gpa'], student['birth']))


def get_students(course_id):
    with pg.connect(dbname='school_db', user='school_user') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select s.id, s.name, c.name from student_course sc
                join student s on s.id = sc.student_id
                join course c on c.id = sc.course_id
                where sc.course_id = %s;
                    """, (course_id,))
            result = cur.fetchall()
    return result


def get_student(student_id):
    with pg.connect(dbname='school_db', user='school_user') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select * from student where id = %s
                    """, (student_id,))
            result = cur.fetchall()
    return result


def get_student_id(name):
    with pg.connect(dbname='school_db', user='school_user') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select id from student where name = %s;
                    """, (name,))
            result = cur.fetchall()
    return result


if __name__ == '__main__':
    add_student({'name': 'Татьяна Голикова', 'gpa': 3.1, 'birth': '1985-10-04'})
    add_course('Экономическая теория', 'Философия', 'Логика')
    add_students(11, {'name': 'Иван Виноградов', 'gpa': 3.6, 'birth': '1986-11-09'},
                    {'name': 'Наталья Степанова', 'gpa': 4.0, 'birth': '1984-06-15'},
                    {'name': 'Анастасия Глаголева', 'gpa': 1.0, 'birth': '1985-05-31'})
    print(get_students(11))
    print(get_student(21))
