import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import db_user, db_password, db_host, db_name


def sql_queries():
    try:
        connection = psycopg2.connect(user=db_user,
                                      password=db_password,
                                      host=db_host,
                                      database=db_name)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        res = cursor.execute(
            f"select room, count(room) as student_count "
            f"from student group by room;"
                             )
        print(res)

    except Exception as error:
        print("[ERROR]", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("[INFO] Connection with Postgres was closed")


sql_queries()
