import unittest
from models import DatabaseConnector
from config import db_user, db_password, db_host, db_name


class DatabaseConnectorTest(unittest.TestCase):
    def setUp(self):
        # Open connection for tests
        self.db_connector = DatabaseConnector(dbname=db_name,
                                              user=db_user,
                                              password=db_password,
                                              host=db_host,
                                              port=5432)
        self.db_connector.connect()

    def tearDown(self):
        # Close connection for tests
        self.db_connector.close()

    def test_connection(self):
        # Test db connection
        self.assertIsNotNone(self.db_connector.connection, "Database connection not established")
        print('--Test for connection passed--\n')

    def test_execute_query(self):
        # Test if a query can be successfully executed
        query = "CREATE TABLE IF NOT EXISTS test_table (id serial PRIMARY KEY, name varchar(255));"
        self.db_connector.execute_query(query)
        self.assertTrue(True, "Query execution successful")
        print('--Test for execute query passed--\n')

    def test_fetch_data_json(self):
        # Test if data can be fetched from the database in JSON format
        query = "SELECT * FROM student WHERE room = 105;"
        data = self.db_connector.fetch_data(query, data_format='json')
        self.assertIsInstance(data, str, "Data not fetched in JSON format")
        print('--Test for fetch data in json passed--\n')

    def test_fetch_data_xml(self):
        # Test if data can be fetched from the database in XML format
        query = "SELECT * FROM student WHERE room = 105;"
        data = self.db_connector.fetch_data(query, data_format='xml')
        self.assertIsInstance(data, str, "Data not fetched in XML format")
        print('--Test fro fetch data in xml passed--\n')

    def test_fetch_data_invalid_format(self):
        # Test if an invalid data format returns an error message
        query = "SELECT * FROM test_table;"
        data = self.db_connector.fetch_data(query, data_format='invalid_format')
        self.assertEqual(data, 'Invalid data format', "Invalid data format not handled correctly")
        print('--Test for invalid data passed--\n')

    def test_create_db(self):
        # Test if a database can be successfully created
        self.db_connector.create_db(dbname='test_db',
                                    user=db_user,
                                    password=db_password,
                                    host=db_host)
        # Check if the new database is created by connecting to it
        db_connector = DatabaseConnector(dbname='test_db',
                                         user=db_user,
                                         password=db_password,
                                         host=db_host,
                                         port=5432)
        db_connector.connect()
        self.assertIsNotNone(db_connector.connection, "Database not created successfully")
        print('--Test for create db passed--\n')


if __name__ == '__main__':
    unittest.main()
