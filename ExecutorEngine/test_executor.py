from executor import SQLExecutor

executor = SQLExecutor()

session_id = "ca4d640a"  # example session id
sql_query = 'SELECT "Project ID", "Project Name", SUM("Posted Hours") FROM sample_table GROUP BY "Project ID", "Project Name";'

result = executor.execute(session_id, sql_query)

if result["success"]:
    print("Columns:", result["columns"])
    print("Rows:", result["rows"])
else:
    print("ERROR:", result["error"])
