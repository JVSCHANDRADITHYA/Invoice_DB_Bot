import duckdb
import threading

data_base_files_path = r'./Database_Files/'


class DuckDBEngine:
    def __init__(self, db_path=':memory:'):
        if not db_path.endswith('.duckdb') and db_path != ':memory:':
            db_path = data_base_files_path + db_path + '.duckdb'
        self.db_path = db_path
        self.lock = threading.Lock()  # Only needed for writes

    def _connect(self):
        return duckdb.connect(self.db_path)

    def execute_query(self, query, is_write=False):
        try:
            # Use fresh connection per call
            conn = self._connect()

            # Writes need lock to avoid catalog race conditions
            if is_write:
                with self.lock:
                    result = conn.execute(query).fetchall()
            else:
                result = conn.execute(query).fetchall()

            conn.close()
            return result
        
        except Exception as e:
            print(f"Query error: {e}")
            return None

    def create_table_from_csv(self, csv_path, table_name):
        query = f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}')
        """
        return self.execute_query(query, is_write=True)

    def fetch_all(self, table_name):
        query = f"SELECT * FROM {table_name}"
        return self.execute_query(query)

    def drop_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name}"
        return self.execute_query(query, is_write=True)

    def list_tables(self):
        return [row[0] for row in self.execute_query("SHOW TABLES") or []]
    

if __name__ == "__main__":
    db_engine = DuckDBEngine('test_db')

    # Create table from CSV
    db_engine.create_table_from_csv(r"F:\Invoice_DB_Bot\data.csv", 'resources')

    # Fetch all data
    data = db_engine.fetch_all('resources')
    print("Data in 'resources' table:")
    for row in data:
        print(row)

    # List tables
    tables = db_engine.list_tables()
    print("Tables in database:", tables)

    # Drop table
    db_engine.drop_table('resources')
    print("Dropped 'resources' table.")