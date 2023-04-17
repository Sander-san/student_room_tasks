import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import db_user, db_password, db_host, db_name


try:
    connection = psycopg2.connect(user=db_user,
                                  password=db_password,
                                  host=db_host)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connection.cursor()
    sqlCreateDatabase = f'create database {db_name};'
    cursor.execute(sqlCreateDatabase)

except Exception as error:
    print("[ERROR]", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("[INFO] Connection with Postgres was closed")
