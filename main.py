from models import DatabaseConnector
from config import db_user, db_password, db_host, db_name
import json


# Create db
DatabaseConnector.create_db(user=db_user, password=db_password, host=db_host, dbname=db_name)

# Object to connect to the db
connector = DatabaseConnector(dbname=db_name, user=db_user, password=db_password, host=db_host, port=5432)

# Open connection
connector.connect()


# Creating tables
query = """CREATE TABLE room(
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
            );
            CREATE TABLE student(
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            sex VARCHAR(1),
            birthday DATE,
            room INTEGER REFERENCES room(id)
            );"""
connector.execute_query(query)


# Add data from json
rooms_json = open('json_data_files/rooms.json').read()
rooms_json_data = json.loads(rooms_json)

for i in rooms_json_data:
    room_id = i.get('id')
    name = i.get('name')
    query = f"INSERT INTO room(id, name) " \
            f"VALUES ('{room_id}', '{name}');"
    connector.execute_query(query)

students_json = open('json_data_files/students.json').read()
students_json_data = json.loads(students_json)

for i in students_json_data:
    student_id = i.get('id')
    name = i.get('name')
    sex = i.get('sex')
    birthday = i.get('birthday')
    room = i.get('room')
    query = f"INSERT INTO student(id, name, sex, birthday, room) " \
            f"VALUES ({student_id}, '{name}', '{sex}', '{birthday}', {room});"
    connector.execute_query(query)


# SQL queries
query1 = "select room, count(room) as student_count " \
         "from student " \
         "group by room " \
         "order by room;"
result1 = connector.fetch_data(query1)

query2 = "select room, ROUND(avg(date_part('year', current_date) - date_part('year', birthday))) as avg_age " \
         "from student " \
         "group by room " \
         "order by avg_age " \
         "limit 5;"
result2 = connector.fetch_data(query2)

query3 = "select room, max(date_part('year', birthday)) - min(date_part('year', birthday)) as age_diff " \
         "from student " \
         "group by room " \
         "order by age_diff desc " \
         "limit 5;"
result3 = connector.fetch_data(query3)

query4 = "select room from student " \
         "group by room " \
         "having count(distinct sex) > 1 order by room;"
result4 = connector.fetch_data(query4)


# Close connection
connector.close()
