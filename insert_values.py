import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import db_user, db_password, db_host, db_name
import json

try:
    connection = psycopg2.connect(user=db_user,
                                  password=db_password,
                                  host=db_host,
                                  database=db_name)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    rooms_json = open('json_data_files/rooms.json').read()
    rooms_json_data = json.loads(rooms_json)

    for i in rooms_json_data:
        room_id = i.get('id')
        name = i.get('name')
        cursor.execute(f"INSERT INTO room(id, name)"
                       f"VALUES ('{room_id}', '{name}');")

    students_json = open('json_data_files/students.json').read()
    students_json_data = json.loads(students_json)

    for i in students_json_data:
        student_id = i.get('id')
        name = i.get('name')
        sex = i.get('sex')
        birthday = i.get('birthday')
        room = i.get('room')
        cursor.execute(f"INSERT INTO student(id, name, sex, birthday, room)"
                       f"VALUES ({student_id}, '{name}', '{sex}', '{birthday}', {room});")


except Exception as error:
    print("[ERROR]", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("[INFO] Connection with Postgres was closed")
