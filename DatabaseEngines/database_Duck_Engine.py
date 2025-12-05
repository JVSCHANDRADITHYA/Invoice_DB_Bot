import duckdb as ddb

data_base_files_path = r'./Database_Files/'

class DuckDBEngine:
    def __init__(self, db_path=':memory:'):
        if not db_path.endswith('.duckdb') and db_path != ':memory:':
            db_path = data_base_files_path + db_path + '.duckdb'
        self.connection = ddb.connect(database=db_path)

    def execute_query(self, query):
        try:
            result = self.connection.execute(query).fetchall()
            return result
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def close(self):
        self.connection.close()

    def create_table_from_csv(self, csv_path, table_name):
        try:
            self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
            print(f"Table '{table_name}' created successfully from '{csv_path}'.")
        except Exception as e:
            print(f"An error occurred while creating table from CSV: {e}")
    
    def fetch_all(self, table_name):
        try:
            result = self.connection.execute(f"SELECT * FROM {table_name}").fetchall()
            return result
        except Exception as e:
            print(f"An error occurred while fetching data from '{table_name}': {e}")
            return None
        
    def drop_table(self, table_name):
        try:
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' dropped successfully.")
        except Exception as e:
            print(f"An error occurred while dropping table '{table_name}': {e}")
            return None
        
    def list_tables(self):
        try:
            result = self.connection.execute("SHOW TABLES").fetchall()
            return [table[0] for table in result]
        except Exception as e:
            print(f"An error occurred while listing tables: {e}")
            return None
        

if __name__ == "__main__":
    db_engine = DuckDBEngine('example.duckdb')
    db_engine.create_table_from_csv(r'F:\Invoice_DB_Bot\data.csv', 'sample_table')
    print(db_engine.fetch_all('sample_table'))
    print(db_engine.list_tables())
    # db_engine.drop_table('sample_table')
    print(db_engine.list_tables())
    db_engine.close()