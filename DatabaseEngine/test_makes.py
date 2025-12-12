from DB_Handler import create_session, create_databases, find_best_match

# 1. Create session
session_id = create_session()
print("Session ID:", session_id)

# 2. Build DBs
duck, chroma = create_databases(session_id, "data.csv")

# 3. Now fuzzy match
result = find_best_match(
    session_id,
    column_name="Resource Name",
    query="Ramya",
)

best = result['best_match']

print("Fuzzy Match Result:", result)

# query the DuckDB to verify
duck_result = duck.execute(f'SELECT * FROM sample_table WHERE "Resource ID" = ?', (best,)).fetchall()
print("DuckDB Result for fuzzy matched value:", duck_result)
