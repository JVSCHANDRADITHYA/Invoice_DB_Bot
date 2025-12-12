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


        
    def interpret_response(self, executor_result, original_user_question):
        """
        Interpret SQLExecutor results into a human-readable explanation
        using the llama.cpp FastAPI backend (non-streaming).
        """

        import json

        # ------------------------
        # SAFE SERIALIZATION
        # ------------------------
        safe_columns = executor_result.get("columns") or []
        safe_rows = executor_result.get("rows") or []

        # Convert tuples → lists for JSON safety
        safe_rows_json = json.dumps([list(r) for r in safe_rows[:20]], ensure_ascii=False)
        safe_columns_json = json.dumps(safe_columns, ensure_ascii=False)

        # ------------------------
        # SYSTEM PROMPT — DO NOT MODIFY
        # ------------------------
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
                STRICT ANTI-HALLUCINATION RULES
===========================================================
• You MUST use values EXACTLY as they appear in the executor output.
• Do NOT modify text, spacing, punctuation, or numeric values.
• Do NOT invent or infer values.
• Only display actual rows exactly as returned.
• If previewing, show rows EXACTLY without alterations.
"""

        # ------------------------
        # USER PAYLOAD
        # ------------------------
        user_payload = f"""
The user originally asked: "{original_user_question}"

Here is the raw executor response that you must interpret:

Success: {executor_result["success"]}
Columns (JSON): {safe_columns_json}
Rows (first 20, JSON): {safe_rows_json}
"""

        # ------------------------
        # COMPOSE FINAL PROMPT (SYSTEM + USER)
        # ------------------------
        final_prompt = f"fOLLOW this sTRICTLY : {SYSTEM_PROMPT}\n\n{user_payload}\n\nWrite a human-friendly explanation IN A Paragraph INTEPRETING THE DATA YOU GOT"

        # ------------------------
        # CALL llama.cpp FastAPI backend
        # ------------------------
        result_text = self.generate(
            final_prompt,
            max_tokens=400,
            temperature=0.2
        )

        return result_text.strip()
