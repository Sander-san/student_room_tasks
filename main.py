from models import DatabaseConnector
from config import db_user, db_password, db_host, db_name
import json
import logging


# add stream and output in file
file_log = logging.FileHandler("test.log")
# add output in console
console_out = logging.StreamHandler()
# add both streams to logger
logging.basicConfig(handlers=(file_log, console_out), level=logging.DEBUG)


# Create db
logging.info(f"Create {db_name} database")
DatabaseConnector.create_db(user=db_user, password=db_password, host=db_host, dbname=db_name)

# Object to connect to the db
logging.info("Create object to connect to the db")
connector = DatabaseConnector(dbname=db_name, user=db_user, password=db_password, host=db_host, port=5432)

# Open connection
logging.info("Open connection with db")
connector.connect()


# Creating tables
logging.info("Creating 2 tables")
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

logging.warning("Add data from rooms_json_data")
for i in rooms_json_data:
    room_id = i.get('id')
    name = i.get('name')
    query = f"INSERT INTO room(id, name) " \
            f"VALUES ('{room_id}', '{name}');"
    # logging.debug(f"Data: {room_id} - {name}")
    connector.execute_query(query)

students_json = open('json_data_files/students.json').read()
students_json_data = json.loads(students_json)

logging.warning("Add data from students_json_data")
for i in students_json_data:
    student_id = i.get('id')
    name = i.get('name')
    sex = i.get('sex')
    birthday = i.get('birthday')
    room = i.get('room')
    query = f"INSERT INTO student(id, name, sex, birthday, room) " \
            f"VALUES ({student_id}, '{name}', '{sex}', '{birthday}', {room});"
    # logging.debug(f"Data: {student_id} | {name} | {sex} | {birthday} | {room}")
    connector.execute_query(query)


# SQL queries
logging.info("Sql query - Список комнат и количество студентов в каждой из них")
query1 = "select room, count(room) as student_count " \
         "from student " \
         "group by room " \
         "order by room;"
result1 = connector.fetch_data(query1, data_format='json')
logging.info(result1)

logging.info("Sql query - 5 комнат, где самый маленький средний возраст студентов")
query2 = "select room, ROUND(avg(date_part('year', current_date) - date_part('year', birthday))) as avg_age " \
         "from student " \
         "group by room " \
         "order by avg_age " \
         "limit 5;"
result2 = connector.fetch_data(query2, data_format='xml')
logging.info(result2)

logging.info("Sql query - 5 комнат с самой большой разницей в возрасте студентов")
query3 = "select room, max(date_part('year', birthday)) - min(date_part('year', birthday)) as age_diff " \
         "from student " \
         "group by room " \
         "order by age_diff desc " \
         "limit 5;"
result3 = connector.fetch_data(query3)
logging.info(result3)

logging.info("Список комнат где живут разнополые студенты")
query4 = "select room from student " \
         "group by room " \
         "having count(distinct sex) > 1 order by room;"
result4 = connector.fetch_data(query4, data_format='json')
logging.info(result4)


# Close connection
connector.close()
