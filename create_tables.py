import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import db_user, db_password, db_host, db_name


try:
    connection = psycopg2.connect(user=db_user,
                                  password=db_password,
                                  host=db_host,
                                  database=db_name)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE room(
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
        )

except Exception as error:
    print("[ERROR]", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("[INFO] Connection with Postgres was closed")
