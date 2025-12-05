import sqlite3
import pandas as pd
import os
from sqlite3 import Error

class Database:
    def __init__(self, default_path = r'./Database_Files/'):
        
        self.default_path = default_path
        if not os.path.exists(self.default_path):
            os.makedirs(self.default_path)
    
    def check_csv_health(self, csv_path):
        """ Check if the CSV file is valid and can be read """
                
        self.csv_path = csv_path

        df = pd.read_csv(csv_path)
        if df.isnull().values.any():
            print(f"CSV file '{csv_path}' contains missing values.")
            return False
        
        try:
            pd.read_csv(csv_path, nrows=5)
            return True
        except Exception as e:
            print(f"Error reading CSV file '{csv_path}': {e}")
            return False
        
    def make_database(csv_path):
        df = pd.read_csv(csv_path)

        df.to_sql(name='data_table', con=conn, if_exists='replace', index=False)
        print(f"Database created from '{csv_path}' successfully.")
        return conn
    
    def create_connection(db_file):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

        return conn
    
        
    


