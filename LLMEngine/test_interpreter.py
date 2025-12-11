from Ollama_Handler import OllamaHandler
import os
# add executor path for import F:\Invoice_DB_Bot\ExecutorEngine
import sys
PROJECT_ROOT = r"F:\Invoice_DB_Bot"
sys.path.append(PROJECT_ROOT)
from ExecutorEngine.executor import SQLExecutor

handler = OllamaHandler()
executor = SQLExecutor()

session_id = "ca4d640a"
sql = 'SELECT "Project ID", "Project Name", SUM("Posted Hours") FROM sample_table GROUP BY "Project ID", "Project Name";'

executor_result = executor.execute(session_id, sql)

interpretation = handler.interpret_response(
    executor_result,
    original_user_question="Show me total posted hours per project"
)

print(interpretation)
