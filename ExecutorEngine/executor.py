import duckdb
import os
import traceback


class SQLExecutor:
    def __init__(self, base_path="Data/sessions"):
        """
        base_path: root directory where session folders are stored.
        """
        self.base_path = base_path


    def _get_duckdb_path(self, session_id):
        """
        Builds the full path to the session's DuckDB file.
        """
        return os.path.join(self.base_path, session_id, "duckdb.duckdb")


    def load_connection(self, session_id):
        """
        Loads (or opens) the DuckDB database for a given session.
        Raises an error if the DB doesn't exist.
        """
        db_path = self._get_duckdb_path(session_id)

        if not os.path.exists(db_path):
            raise FileNotFoundError(
                f"No DuckDB database found for session_id='{session_id}' at {db_path}"
            )

        return duckdb.connect(db_path)


    def execute(self, session_id, sql_query):
        """
        Executes an SQL query on the DuckDB DB for a given session.
        
        Returns:
            {
                "success": bool,
                "columns": list[str] or None,
                "rows": list[tuple] or None,
                "error": str or None
            }
        """
        try:
            conn = self.load_connection(session_id)

            # Execute SQL
            result = conn.execute(sql_query)

            # Fetch results (DuckDB: fetchall returns list of tuples)
            rows = result.fetchall()
            
            # Get column names
            columns = [col[0] for col in result.description]

            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "error": None
            }

        except Exception as e:
            return {
                "success": False,
                "columns": None,
                "rows": None,
                "error": f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            }
