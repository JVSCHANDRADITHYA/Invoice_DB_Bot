import requests

url = "http://localhost:11434/api/chat?stream=false"

import json
import requests
SQL_SYSTEM_PROMPT = SQL_SYSTEM_PROMPT = """
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
import json
import requests

url = "http://localhost:11434/api/chat"


class OllamaHandler:
    def __init__(self, model="gpt-oss", temperature=0.0, max_tokens=256):
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens


    def generate_SQL(self, user_prompt):
        headers = {"Content-Type": "application/json"}

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SQL_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens
        }

        # Ollama ALWAYS streams → must use stream=True
        response = requests.post(url, json=payload, headers=headers, stream=True)
        response.raise_for_status()

        final_output = ""

        for line in response.iter_lines():
            if not line:
                continue

            try:
                obj = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                continue  # skip malformed chunks

            # Append streamed LLM content
            if "message" in obj and "content" in obj["message"]:
                final_output += obj["message"]["content"]

        # Clean & return SQL
        return self.clean_SQL(final_output)


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
    
    def interpret_response(self, executor_response, original_user_question):
        """
        Interprets the result returned by the SQLExecutor using the LLM.
        
        executor_response structure:
        {
            "success": True/False,
            "columns": [...],
            "rows": [...],
            "error": "..." or None
        }
        """

        SYSTEM_PROMPT = """
You are a Data Interpretation Assistant designed to translate raw database query
results into clear, accurate, human-readable insights.

===========================================================
                   BACKGROUND CONTEXT
===========================================================
A user provides a natural-language question such as:
"Get total posted hours per project" or
"Show all resources who logged hours after actual date."

The system converts the question into SQL using an LLM.
That SQL query is executed by the Execution Engine, which queries a DuckDB
database and returns structured output.

Your job is to interpret that structured output and explain
the meaning of the results clearly to the user.

===========================================================
         EXECUTION ENGINE RESPONSE STRUCTURE
===========================================================
You will receive this dictionary:

{
    "success": bool,
    "columns": list[str] or None,
    "rows": list[tuple] or None,
    "error": str or None
}

• If success = True:
    - columns = names of the output fields
    - rows = actual data returned
    - You must interpret the results.

• If success = False:
    - error = textual description of what failed.
    - You must explain the error clearly, in simple terms.

===========================================================
                HOW TO INTERPRET RESULTS
===========================================================
When success=True:
    • Provide a helpful explanation of what the user asked.
    • Summarize what the returned rows represent.
    • Highlight key values, totals, or patterns if relevant.
    • Format tabular data clearly if needed.
    • If many rows exist, summarize but show a small preview.
    • NEVER invent facts not present in the columns/rows.

When success=False:
    • Explain the error in simple language.
    • Suggest how the user may correct the query.
    • Do NOT show internal stack traces unless helpful.

===========================================================
                 STYLE AND TONE GUIDELINES
===========================================================
• Clear, concise, human-readable.
• Professional and neutral tone.
• Do NOT output SQL.
• Do NOT regenerate SQL.
• Do NOT hallucinate column names or values.

===========================================================

===========================================================
                STRICT ANTI-HALLUCINATION RULES
===========================================================
When presenting results:

• You MUST use values EXACTLY as they appear in the executor output.
• Do NOT modify text, spelling, spacing, punctuation, or numeric values.
• Do NOT invent or fabricate any values.
• Do NOT shorten, abbreviate, summarize, or infer missing values.
• If a value is empty, display it exactly as empty.
• If there are more rows than you can show, list only a few rows EXACTLY and
  state that you are showing a preview.

When creating tables:

• Use only the rows that appear in the executor response.
• Show them exactly as-is, without altering any characters.
• Do not invent new rows or remove valid ones.

These anti-hallucination rules MUST be followed every time.

"""
        user_payload = f"""
The user originally asked: "{original_user_question}"

Here is the raw executor response that you must interpret:

{json.dumps(executor_response, indent=4)}
"""

        headers = {"Content-Type": "application/json"}
        payload = {
            "model": 'llama3.2:3b',
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_payload}
            ],
            "temperature": 0.2,
            "max_tokens": 400
        }

        # IMPORTANT → STREAMING MODE (your Ollama ALWAYS streams)
        response = requests.post(
            "http://localhost:11434/api/chat",
            json=payload,
            headers=headers,
            stream=True
        )
        response.raise_for_status()

        final_output = ""

        # Parse NDJSON streaming chunks
        for line in response.iter_lines():
            if not line:
                continue

            try:
                obj = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            if "message" in obj and "content" in obj["message"]:
                final_output += obj["message"]["content"]

        return final_output.strip()
