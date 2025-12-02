'''
Docstring for database_connector
connect a sqldb databse that tis to be generated from a csv file
'''
import sqlite3
import pandas as pd
from sqlite3 import Error
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to database: {db_file}")
    except Error as e:
        print(e)
    return conn

def create_table_from_csv(conn, csv_file, table_name):
    """ create a table from a CSV file """
    try:
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Table '{table_name}' created from '{csv_file}'")
    except Exception as e:
        print(e)

def main():
    database = "sample_erp.db"
    csv_file = "data.csv"
    table_name = "hours"

    # create a database connection
    conn = create_connection(database)

    # create table from csv
    if conn is not None:
        create_table_from_csv(conn, csv_file, table_name)
        conn.close()
    else:
        print("Error! cannot create the database connection.")
    
    # query to database and pritn results
    try:
        conn = create_connection(database)
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT([Project Name]) FROM {table_name}")

        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()