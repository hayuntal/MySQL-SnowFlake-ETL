import mysql.connector
import os

from mysql.connector import Error


class MySqlClient:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                db_info = self.connection.get_server_info()
                print(f"Successfully connected to MySQL Server version {db_info}")
                cursor = self.connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
        except Error as e:
            print("Error while connecting to MySQL", e)

    def fetch_data(self, table):
        """ Execute a query and return the results """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT * FROM {table};")
            result = cursor.fetchall()  # Fetch all rows of a query result
            return result
        except Error as e:
            print(f"Error: {e}")
            return None

    def fetch_table_names(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW TABLES FROM `{self.database}`;")
            tables = cursor.fetchall()
            return [table[0] for table in tables]  # Extract table names from tuples
        except Error as e:
            print("Error fetching table names:", e)
            return []

    def fetch_column_names(self, table_name):
        """Fetch all column names from a specified table"""
        
        query = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{self.database}'
                AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION;
            """

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            columns = cursor.fetchall()
            return [column[0] for column in columns]
        except Error as e:
            print(f"Error fetching column names: {e}")
            return []