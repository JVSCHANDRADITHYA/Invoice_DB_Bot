import os
import uuid
import duckdb
import chromadb
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_community.document_loaders import CSVLoader
from rapidfuzz import process, fuzz


BASE_PATH = "Data/sessions"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def create_session():
    """Creates a unique session id and folder."""
    session_id = str(uuid.uuid4())[:8]  # short session id
    session_path = f"{BASE_PATH}/{session_id}"
    os.makedirs(session_path, exist_ok=True)
    return session_id


def get_paths(session_id):
    path = f"{BASE_PATH}/{session_id}"
    return {
        "root": path,
        "duckdb": f"{path}/duckdb.duckdb",
        "chroma": f"{path}/chroma"
    }


# ---------------------------------------------------------
#                CREATE DUCKDB + CHROMA
# ---------------------------------------------------------

def create_databases(session_id, csv_path):
    paths = get_paths(session_id)

    # 1. Create DuckDB
    conn = duckdb.connect(paths["duckdb"])
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS sample_table AS
        SELECT * FROM read_csv_auto('{csv_path}')
    """)

    # 2. Create Chroma
    embeddings = HuggingFaceEmbeddings(model_name=EMBED_MODEL)
    loader = CSVLoader(file_path=csv_path)
    documents = loader.load()

    vector_db = Chroma.from_documents(
        documents,
        embeddings,
        persist_directory=paths["chroma"],
        collection_name="info_collection"
    )  # auto-persist

    return conn, vector_db


# ---------------------------------------------------------
#               ACCESS EXISTING SESSION DBs
# ---------------------------------------------------------

def get_duckdb(session_id):
    paths = get_paths(session_id)
    return duckdb.connect(paths["duckdb"])


def get_chroma(session_id):
    paths = get_paths(session_id)
    embeddings = HuggingFaceEmbeddings(model_name=EMBED_MODEL)

    return Chroma(
        embedding_function=embeddings,
        persist_directory=paths["chroma"],
        collection_name="info_collection"
    )


# ---------------------------------------------------------
#                FUZZY MATCHING ENGINE
# ---------------------------------------------------------

def find_best_match(session_id, column_name, query):
    """
    Returns the closest matching value from the CSV loaded into DuckDB.
    """
    conn = get_duckdb(session_id)

    # fetch column values
    result = conn.execute(f"SELECT \"{column_name}\" FROM sample_table").fetchall()
    values = [r[0] for r in result]

    # fuzzy match
    match, score, idx = process.extractOne(
        query,
        values,
        scorer=fuzz.WRatio
    )
    
    # for debug purposes, we can return more info
    return {
        "input": query,
        "best_match": match,
        "score": score
    }
