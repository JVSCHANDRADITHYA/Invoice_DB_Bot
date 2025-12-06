"""
nl_to_sql_vector_rules.py

Deterministic NL -> SQL for your `sample_table` schema.
+ Vector DB (FAISS) fuzzy name matching for Project Name & Resource Name.
"""

import re
from typing import Optional, Dict, List

import faiss
from sentence_transformers import SentenceTransformer


# ================================================================
#  VECTOR INDEX CLASS
# ================================================================
class NameIndex:
    def __init__(self, names_list: List[str], threshold=0.45):
        """
        names_list: actual valid names from DB
        threshold: similarity cutoff
        """
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.names = names_list
        self.threshold = threshold
        self.index = self._build(names_list)

    def _build(self, names):
        embeddings = self.model.encode(names, normalize_embeddings=True)
        self.embeddings = embeddings
        dim = embeddings.shape[1]

        index = faiss.IndexFlatIP(dim)  # cosine SIM
        index.add(embeddings)
        return index

    def nearest(self, query: str):
        """
        Returns: (best_name, score)
        If no good match, returns (query, 0)
        """
        q_emb = self.model.encode([query], normalize_embeddings=True)
        scores, idx = self.index.search(q_emb, 1)

        score = float(scores[0][0])
        best_idx = int(idx[0][0])

        if score < self.threshold:
            return query, score  # return raw fallback

        return self.names[best_idx], score


# ================================================================
#  SCHEMA COLUMNS
# ================================================================
SCHEMA_COLUMNS = [
    "Project Financial Location",
    "Project ID",
    "Project Name",
    "Project Manager",
    "Resource Name",
    "Resource ID",
    "Resource Financial Location",
    "Posted Hours",
    "Project Task Name",
    "Project Task ID",
    "Actual Date",
    "Posted Date",
    "Financial Period (Posted Date)",
    "Resource Financial Department",
    "Project Financial Department",
    "Project Class",
    "Timesheet Week (Actual Date)",
    "Timesheet Week (Posted Date)",
    "Resource Rate",
    "Project Rate",
    "Resource Primary Role",
    "Resource Project Role",
    "Resource Currency",
]


def quote(col: str) -> str:
    return f'"{col}"'


# ================================================================
#  VOCAB → COLUMN MAP
# ================================================================
KEYWORD_COLUMN_MAP = {
    "project": "Project Name",
    "project name": "Project Name",
    "project id": "Project ID",
    "project manager": "Project Manager",
    "manager": "Project Manager",
    "resource": "Resource Name",
    "resource name": "Resource Name",
    "resource id": "Resource ID",
    "posted hours": "Posted Hours",
    "posted date": "Posted Date",
    "actual date": "Actual Date",
    "financial period": "Financial Period (Posted Date)",
    "resource rate": "Resource Rate",
    "project class": "Project Class",
}


# ================================================================
#  PATTERNS
# ================================================================
AGGREGATION_PATTERNS = {
    "count_distinct_actual_days": re.compile(r"\bhow many days\b|\bnumber of days\b", re.I),
    "count": re.compile(r"\bhow many\b|\bcount\b|\bnumber of\b", re.I),
    "sum": re.compile(r"\btotal\b|\bsum\b|\btotal of\b|\bamount\b", re.I),
    "list": re.compile(r"\blist\b|\bshow rows\b|\bshow\b|\bdisplay\b", re.I),
    "select_single": re.compile(r"\bwho is\b|\bwho's\b|\bwhat is\b", re.I),
}

ID_PATTERN = re.compile(r"\b([A-Z]{1,5}_[A-Z]{1,5}_[0-9]{1,5})\b")


# ================================================================
#  VECTOR INDEXES init (you must load real values from DB)
# ================================================================
# PLACEHOLDER — set actual values!
project_name_index = NameIndex(["Project A", "Project B", "Raghavarapu Ramyashree"])
resource_name_index = NameIndex(["HY_RR_01", "Raghavarapu Ramyashree", "John Doe"])


# ================================================================
#  FIND COLUMN FROM PHRASE
# ================================================================
def find_column_by_phrase(phrase: str) -> Optional[str]:
    p = phrase.lower().strip()
    if p in KEYWORD_COLUMN_MAP:
        return KEYWORD_COLUMN_MAP[p]

    for k, col in KEYWORD_COLUMN_MAP.items():
        if k in p or p in k:
            return col

    words = set(re.findall(r"[a-zA-Z0-9]+", p))
    for col in SCHEMA_COLUMNS:
        col_words = set(re.findall(r"[a-zA-Z0-9]+", col.lower()))
        if words & col_words:
            return col
    return None


# ================================================================
#  PARSE NATURAL LANGUAGE → INTENT
# ================================================================
def parse_nl_query(nl: str) -> Dict:
    nl0 = nl.strip()
    lower = nl0.lower()

    out = {
        "agg": None,
        "select": None,
        "filters": [],
        "group_by": None,
        "raw": nl0,
    }

    # ----------------------------
    #  AGGREGATION DETECTION
    # ----------------------------
    if AGGREGATION_PATTERNS["count_distinct_actual_days"].search(lower):
        out["agg"] = "count_distinct_actual_days"
        out["select"] = quote("Actual Date")

    elif AGGREGATION_PATTERNS["count"].search(lower):
        out["agg"] = "count"
        if "resource" in lower:
            out["select"] = quote("Resource ID")
        else:
            out["select"] = "*"

    elif AGGREGATION_PATTERNS["sum"].search(lower):
        out["agg"] = "sum"
        out["select"] = quote("Posted Hours")

    elif AGGREGATION_PATTERNS["list"].search(lower):
        out["agg"] = "list"
        out["select"] = "*"

    elif AGGREGATION_PATTERNS["select_single"].search(lower):
        out["select"] = quote("Project Manager")

    # ----------------------------
    #  ID extraction
    # ----------------------------
    m = ID_PATTERN.search(nl0)
    if m:
        out["filters"].append((quote("Resource ID"), "=", m.group(1)))

    # ----------------------------
    #  NAME after "of ..."
    # ----------------------------
    m4 = re.search(r"of\s+([A-Za-z0-9 '\-]+)\??$", nl0)
    if m4:
        raw_name = m4.group(1).strip()

        # === VECTOR DETECTION ===
        proj_best, proj_score = project_name_index.nearest(raw_name)
        res_best, res_score = resource_name_index.nearest(raw_name)

        if proj_score > res_score:
            out["filters"].append((quote("Project Name"), "=", proj_best))
        else:
            out["filters"].append((quote("Resource Name"), "=", res_best))

    # ----------------------------
    #  DATE comparison
    # ----------------------------
    if "later than" in lower or "after" in lower:
        out["filters"].append((quote("Posted Date"), ">", quote("Actual Date")))

    # ----------------------------
    #  Financial period YYYY-MM
    # ----------------------------
    fp = re.search(r"(\b20\d{2}-\d{2}\b)", nl0)
    if fp:
        out["filters"].append((quote("Financial Period (Posted Date)"), "=", fp.group(1)))

    # ----------------------------
    #  NUMERIC RATE
    # ----------------------------
    mnum = re.search(r"resource rate.*?(>|>=|<|<=|=)\s*([0-9.]+)", nl0, re.I)
    if mnum:
        op = mnum.group(1)
        val = mnum.group(2)
        out["filters"].append((quote("Resource Rate"), op, val))

    return out


# ================================================================
#  BUILD SQL FROM INTENT
# ================================================================
def build_sql_from_intent(intent: Dict) -> str:
    agg = intent["agg"]
    select = intent["select"]
    filters = intent["filters"]

    # ----- WHERE clause -----
    where_clauses = []
    for col, op, val in filters:
        if isinstance(val, str) and val.startswith('"') and val.endswith('"'):
            rhs = val
        elif isinstance(val, str) and re.match(r"^[A-Za-z0-9_]+$", val):
            rhs = f"'{val}'"
        else:
            escaped = val.replace("'", "''")
            rhs = f"'{escaped}'"

        where_clauses.append(f"{col} {op} {rhs}")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # ----- SELECT by aggregation -----
    if agg == "count_distinct_actual_days":
        return f"""SELECT COUNT(DISTINCT "Actual Date") AS days_logged_later
FROM sample_table
{where_sql};"""

    if agg == "count":
        if select == "*":
            sel = "COUNT(*) AS cnt"
        else:
            sel = f"COUNT(DISTINCT {select}) AS cnt"
        return f"""SELECT {sel}
FROM sample_table
{where_sql};"""

    if agg == "sum":
        return f"""SELECT SUM({select}) AS total
FROM sample_table
{where_sql};"""

    if agg == "list" or agg is None:
        if select is None:
            select = "*"
        return f"""SELECT {select}
FROM sample_table
{where_sql};"""

    # fallback
    return f"""SELECT {select or '*'}
FROM sample_table
{where_sql};"""


# ================================================================
#  PUBLIC FUNCTION
# ================================================================
def nl_to_sql(query: str) -> str:
    intent = parse_nl_query(query)
    sql = build_sql_from_intent(intent)
    return sql


# ================================================================
#  DEMO
# ================================================================
if __name__ == "__main__":
    tests = [
        "whos is project manager of raghavarapu ramyashree?",
        "Who is the project manager of Ramyashree Raghavarapu?",
        "for how many days the resource logged hours for a date later than the actual date for the resource with ID HY_RR_01",
        "what is the total posted hours for project A in financial period 2023-05 for the resource Raghavarapu Ramyashree?",
        "list all entries where resource rate is greater than 50",
        "how many resources are there?",
        "show me all the data for project B",
    ]

    for t in tests:
        print("Q:", t)
        print("SQL:", nl_to_sql(t), "\n---")
