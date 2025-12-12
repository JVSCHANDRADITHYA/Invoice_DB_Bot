import requests
import json

SQL_SYSTEM_PROMPT = """
You are an SQL query generator.
Your ONLY job is to output a valid SQL query.

===========================
      STRICT RULES
===========================
1. Output ONLY the SQL query.
2. Do NOT add explanations, notes, markdown, or backticks.
3. Do NOT rewrite the schema.
4. Do NOT add comments in the SQL.
5. Use ONLY the columns exactly as they appear in the schema.
6. Use ONLY the table name: sample_table.
7. If the user provides meanings for columns, use them ONLY for query logic.
8. If a request is ambiguous, generate the simplest valid SQL query.
9. NEVER invent columns, tables, or functions that do not exist.
10. All dates are real DATE types unless shown as VARCHAR.
11. Assume queries run on DuckDB unless the user specifies otherwise.

===========================
     CHAIN-OF-THOUGHT
===========================
Do NOT reveal chain-of-thought or internal reasoning steps.
Do NOT provide your chain-of-thought. Provide only the final SQL query.

===========================
         SCHEMA
===========================
Columns in table sample_table are:

- Project Financial Location (VARCHAR)
- Project ID (VARCHAR)
- Project Name (VARCHAR)
- Project Manager (VARCHAR)
- Resource Name (VARCHAR)
- Resource ID (VARCHAR)
- Resource Financial Location (VARCHAR)
- Posted Hours (DOUBLE)
- Project Task Name (VARCHAR)
- Project Task ID (VARCHAR)
- Actual Date (DATE)
- Posted Date (DATE)
- Financial Period (Posted Date) (VARCHAR, format: YYYY-MM)
- Resource Financial Department (VARCHAR)
- Project Financial Department (VARCHAR)
- Project Class (VARCHAR)
- Timesheet Week (Actual Date) (VARCHAR)
- Timesheet Week (Posted Date) (VARCHAR)
- Resource Rate (DOUBLE)
- Project Rate (VARCHAR)
- Resource Primary Role (VARCHAR)
- Resource Project Role (VARCHAR)
- Resource Currency (VARCHAR)

===========================
     COLUMN MEANINGS
===========================
Use these ONLY when required for logic:

- Actual Date:
  The date on which the work/hours actually occurred.

- Posted Date:
  The date on which the resource logged the hours into the system.
  A resource may log hours for a past Actual Date at a later Posted Date.

- Financial Period (Posted Date):
  A string like "2025-11" representing the financial month of the Posted Date.

All other fields (Project ID, Resource Name, Posted Hours, etc.) behave normally.

===========================
     FEW-SHOT EXAMPLES
===========================

User: Get total Posted Hours per Resource Name.
Assistant:
SELECT "Resource Name", SUM("Posted Hours") AS total_hours
FROM sample_table
GROUP BY "Resource Name";

User: Show rows where Posted Date is after Actual Date.
Assistant:
SELECT *
FROM sample_table
WHERE "Posted Date" > "Actual Date";

User: Count number of entries per Financial Period (Posted Date).
Assistant:
SELECT "Financial Period (Posted Date)", COUNT(*) AS entry_count
FROM sample_table
GROUP BY "Financial Period (Posted Date)";
"""

class LlamaCPPHandler:
    def __init__(self, api_url="http://localhost:8001/generate"):
        self.api_url = api_url

    def generate(self, prompt, max_tokens=256, temperature=0.1):
        """
        Basic text generation from FastAPI + llama.cpp service.
        """
        payload = {
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        r = requests.post(self.api_url, json=payload)
        r.raise_for_status()
        return r.json()["text"]

    def generate_SQL(self, question):
        """
        Wrap the SQL system prompt + user question.
        """  # your existing prompt

        final_prompt = f"{SQL_SYSTEM_PROMPT}\n\nUSER QUERY:\n{question}\n\nSQL ONLY:"
        response = self.generate(final_prompt, max_tokens=300, temperature=0.0)

        # Return cleaned SQL
        return self.clean_SQL(response)

    def clean_SQL(self, sql):
        """
        Extract a valid SQL query from the streamed output.
        """

        sql = sql.replace("```sql", "").replace("```", "")

        lines = sql.split("\n")
        collecting = False
        final_lines = []

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Start capturing once SELECT or WITH appears
            if stripped.upper().startswith("SELECT") or stripped.upper().startswith("WITH"):
                collecting = True

            if collecting:
                final_lines.append(stripped)

                # Stop when a line ends with semicolon
                if stripped.endswith(";"):
                    break

        return " ".join(final_lines).strip()

    def interpret_response(self, executor_result, original_question):
        """
        Turns SQL result table → natural language summary.
        """
        prompt = f"""
You are an AI assistant. A SQL query was executed.

User question:
{original_question}

SQL Execution Result:
Success: {executor_result["success"]}
Columns: {executor_result.get("columns")}
Rows (first 20): {executor_result.get("rows")[:20] if executor_result.get("rows") else []}

Write a human-friendly explanation of this result.
"""

        return self.generate(prompt, max_tokens=400)
