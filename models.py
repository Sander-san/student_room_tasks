import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import xml.etree.ElementTree as ET


class DatabaseConnector:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        """Connection to database"""
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print(f"Connection to {self.dbname} was successfully")
        except psycopg2.Error as e:
            print(f"[ERROR] with connection to db: {e}")

    def close(self):
        """Method for closing db connection"""
        if self.connection:
            self.connection.close()
            print(f"Connection with {self.dbname} was closed")
            self.connection = None

    def execute_query(self, query, values=None):
        """Method for executing the SQL query"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, values)
            self.connection.commit()
            print("The request was successful")
        except psycopg2.Error as e:
            print(f"Request execution error: {e}")
            self.connection.rollback()

    def fetch_data(self, query, values=None, data_format=None):
        """SQL query to getting data"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, values)
            rows = cursor.fetchall()
            if not data_format:
                return rows
            else:
                if data_format == 'json':
                    columns = [items[0] for items in cursor.description]
                    result = []
                    for row in rows:
                        result.append(dict(zip(columns, row)))
                    cursor.close()
                    print('Data was successfully converted to json')
                    return json.dumps(result, indent=2)
                elif data_format == 'xml':
                    columns = [items[0] for items in cursor.description]
                    root = ET.Element('data')  # create root element
                    for row in rows:
                        row_element = ET.SubElement(root, 'row')  # create sub element in root
                        for i, value in enumerate(row):
                            column_name = columns[i]
                            column_element = ET.SubElement(row_element, column_name)
                            column_element.text = str(value)
                    cursor.close()
                    print('Data was successfully converted to xml')
                    return ET.tostring(root, encoding='utf-8').decode()
                else:
                    return 'Invalid data format'
        except psycopg2.Error as e:
            print(f"Request execution error: {e}")

    @staticmethod
    def create_db(user, password, host, dbname):
        connection = None
        cursor = None

        try:
            connection = psycopg2.connect(user=user,
                                          password=password,
                                          host=host)
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            query = f'create database {dbname};'
            cursor.execute(query)
            print(f"Database {dbname} successfully created")

        except psycopg2.Error as e:
            print("[ERROR]", e)

        finally:
            if connection:
                cursor.close()
                connection.close()
                print("[INFO] Connection with Postgres was closed")
