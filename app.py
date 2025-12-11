# streamlit_app.py
import os
import uuid
import json
import datetime
import tempfile
import shutil

import streamlit as st
import pandas as pd

# Project module imports (assumes packages exist with __init__.py)
from ExecutorEngine.executor import SQLExecutor
from LLMEngine.Ollama_Handler import OllamaHandler
from InvoiceEngine.Invoicer import Invoicer

# Optional PDF conversion (docx -> pdf). If not present, app will continue.
try:
    from docx2pdf import convert as docx2pdf_convert
    DOCX2PDF_AVAILABLE = True
except Exception:
    DOCX2PDF_AVAILABLE = False

# Constants
BASE_DATA_PATH = "Data/sessions"
INVOICES_PATH = "Invoices"
os.makedirs(BASE_DATA_PATH, exist_ok=True)
os.makedirs(INVOICES_PATH, exist_ok=True)

st.set_page_config(page_title="Invoice/Query Manager", layout="wide")


# -------------------------
# Utility helpers
# -------------------------
def create_session():
    sid = uuid.uuid4().hex[:8]
    session_path = os.path.join(BASE_DATA_PATH, sid)
    os.makedirs(session_path, exist_ok=True)
    return sid, session_path


def save_uploaded_csv(uploaded_file, dest_path):
    # Save StreamlitUploadedFile to destination path
    with open(dest_path, "wb") as f:
        f.write(uploaded_file.getbuffer())


def create_duckdb_from_csv(session_path, csv_path):
    """
    Creates duckdb file at <session_path>/duckdb.duckdb and creates table sample_table
    using read_csv_auto.
    """
    import duckdb
    db_path = os.path.join(session_path, "duckdb.duckdb")
    conn = duckdb.connect(db_path)
    # Remove previous table if exists
    conn.execute("DROP TABLE IF EXISTS sample_table")
    # Create sample_table
    conn.execute(f"CREATE TABLE sample_table AS SELECT * FROM read_csv_auto('{csv_path}')")
    conn.close()
    return db_path


def run_sql_and_interpret(session_id, sql_query, original_user_question=None):
    executor = SQLExecutor()
    executor_result = executor.execute(session_id, sql_query)
    # Display result table if success
    if executor_result["success"]:
        df = pd.DataFrame(executor_result["rows"], columns=executor_result["columns"])
    else:
        df = None

    # Interpret using Ollama
    handler = OllamaHandler()
    interpretation = handler.interpret_response(executor_result, original_user_question or sql_query)

    return executor_result, df, interpretation


def safe_convert_to_pdf(docx_path, pdf_path):
    """Attempt to convert docx -> pdf using docx2pdf if available"""
    if not DOCX2PDF_AVAILABLE:
        return False, "docx2pdf not available on this system."
    try:
        docx2pdf_convert(docx_path, pdf_path)
        return True, None
    except Exception as e:
        return False, str(e)


# -------------------------
# UI: Navigation
# -------------------------
st.title("Invoice & Query Manager")
page = st.sidebar.selectbox("Select Page", ["Home / Upload & Query", "Invoice Generator", "Sessions"])

# -------------------------
# PAGE: Home / Upload & Query
# -------------------------
if page == "Home / Upload & Query":
    st.header("Upload CSV (creates session-specific databases)")
    col1, col2 = st.columns([2, 1])

    with col1:
        uploaded = st.file_uploader("Upload CSV file", type=["csv"])
        if uploaded is not None:
            # create session
            sid, session_path = create_session()
            st.success(f"Created new session: {sid}")
            csv_path = os.path.join(session_path, "data.csv")
            save_uploaded_csv(uploaded, csv_path)
            st.write("Saved CSV to:", csv_path)

            # Preview CSV
            try:
                df_preview = pd.read_csv(csv_path, nrows=200)
                st.subheader("CSV Preview (first 200 rows)")
                st.dataframe(df_preview)
            except Exception as e:
                st.error(f"Failed to parse CSV preview: {e}")

            # Create DuckDB
            try:
                db_path = create_duckdb_from_csv(session_path, csv_path)
                st.success(f"DuckDB created at: {db_path}")
            except Exception as e:
                st.error(f"Failed creating DuckDB: {e}")

            # Store active session in session_state
            st.session_state["active_session"] = sid
            st.session_state["session_path"] = session_path

    with col2:
        st.subheader("Active session")
        active = st.session_state.get("active_session")
        if active:
            st.write("Session id:", active)
            st.write("Session path:", st.session_state.get("session_path"))
            if st.button("Clear session"):
                st.session_state.pop("active_session", None)
                st.session_state.pop("session_path", None)
                st.experimental_rerun()
        else:
            st.info("Upload a CSV to create a session.")

    # -------------------------
    # SQL / NL -> SQL box
    # -------------------------
    st.markdown("---")
    st.header("Ask a question or enter SQL")
    handler = OllamaHandler()  # local instance

    question = st.text_input("Enter natural language question (optional). If left empty, enter raw SQL below.")
    raw_sql = st.text_area("Enter raw SQL (optional). If you provided a question, the model will generate SQL.")

    if st.button("Run Query"):
        sid = st.session_state.get("active_session")
        if not sid:
            st.error("No active session. Please upload a CSV first.")
        else:
            # decide SQL source
            if raw_sql.strip():
                sql_to_run = raw_sql.strip()
                original_question = raw_sql.strip()
            elif question.strip():
                # call LLM to generate SQL
                try:
                    # generate_SQL handles streaming; returns cleaned SQL
                    sql_to_run = handler.generate_SQL(question)
                    original_question = question
                except Exception as e:
                    st.error(f"LLM failed to generate SQL: {e}")
                    sql_to_run = None
                    original_question = question
            else:
                st.error("Please enter a question or a raw SQL query.")
                sql_to_run = None
                original_question = None

            if sql_to_run:
                st.subheader("Generated SQL")
                st.code(sql_to_run)

                # Execute + interpret
                with st.spinner("Executing query..."):
                    exec_res, df_result, interp = run_sql_and_interpret(sid, sql_to_run, original_question)

                if exec_res["success"]:
                    st.subheader("Query Result (first 1000 rows)")
                    st.dataframe(df_result.head(1000))
                else:
                    st.error(f"Query execution failed: {exec_res['error']}")

                st.subheader("Interpretation")
                st.markdown(interp)

# -------------------------
# PAGE: Invoice Generator
# -------------------------
elif page == "Invoice Generator":
    st.header("Invoice Generator")

    # Choose session
    sessions = [d for d in os.listdir(BASE_DATA_PATH) if os.path.isdir(os.path.join(BASE_DATA_PATH, d))]
    sessions.sort(reverse=True)
    chosen_session = st.selectbox("Select session", options=["-- new session --"] + sessions)

    if chosen_session == "-- new session --":
        st.warning("Upload a CSV on Home page to create a session OR select an existing session.")
    else:
        sid = chosen_session
        session_path = os.path.join(BASE_DATA_PATH, sid)
        # Connect to DB and fetch distinct resource/project lists
        try:
            import duckdb
            conn = duckdb.connect(os.path.join(session_path, "duckdb.duckdb"))
            resources = [r[0] for r in conn.execute('SELECT DISTINCT "Resource Name" FROM sample_table').fetchall() if r[0] is not None]
            projects = [r[0] for r in conn.execute('SELECT DISTINCT "Project Name" FROM sample_table').fetchall() if r[0] is not None]
            conn.close()
        except Exception as e:
            st.error(f"Failed to read DB for session {sid}: {e}")
            resources, projects = [], []

        col1, col2 = st.columns(2)
        with col1:
            resource_choice = st.selectbox("Select Resource (exact)", options=["-- pick or fuzzy input --"] + resources)
            resource_fuzzy = st.text_input("Or type resource name (fuzzy)")

        with col2:
            project_choice = st.selectbox("Select Project (exact)", options=["-- pick or fuzzy input --"] + projects)
            project_fuzzy = st.text_input("Or type project name (fuzzy)")

        # Decide final selections (prefer exact pick)
        final_resource = None
        final_project = None

        if resource_choice and resource_choice != "-- pick or fuzzy input --":
            final_resource = resource_choice
        elif resource_fuzzy:
            # fuzzy match locally (simple top-1)
            from rapidfuzz import process, fuzz
            candidates = [r for r in resources]
            if not candidates:
                st.error("No resource values available in DB to fuzzy-match.")
            else:
                best = process.extractOne(resource_fuzzy, candidates, scorer=fuzz.WRatio)
                final_resource = best[0]
                st.info(f"Fuzzy matched Resource: {final_resource} (score={best[1]})")

        if project_choice and project_choice != "-- pick or fuzzy input --":
            final_project = project_choice
        elif project_fuzzy:
            from rapidfuzz import process, fuzz
            candidates = [p for p in projects]
            if not candidates:
                st.error("No project values available in DB to fuzzy-match.")
            else:
                best = process.extractOne(project_fuzzy, candidates, scorer=fuzz.WRatio)
                final_project = best[0]
                st.info(f"Fuzzy matched Project: {final_project} (score={best[1]})")

        st.markdown("---")
        period_input = st.text_input("Enter Financial Period (month name or YYYY-MM). Example: 'November' or '2025-11'")

        if st.button("Compute & Preview Invoice"):
            if not final_resource or not final_project:
                st.error("Please choose or fuzzy-find both resource and project.")
            else:
                invoicer = Invoicer(session_id=sid, base_path=BASE_DATA_PATH, invoice_path=INVOICES_PATH)
                try:
                    period_conv = invoicer.convert_period(period_input)
                except Exception as e:
                    st.error(f"Failed to interpret period: {e}")
                    period_conv = None

                if period_conv:
                    fin = invoicer.compute_financials(final_resource, final_project, period_conv)
                    st.subheader("Computed Financials")
                    st.write(fin)
                    st.info("If results look good, click Generate Invoice to create DOCX/JSON (PDF conversion attempted if available).")

                    if st.button("Generate Invoice"):
                        with st.spinner("Generating invoice..."):
                            out_docx, meta_json = invoicer.generate_invoice(
                                resource_name=final_resource,
                                project_name=final_project,
                                project_id=final_project,  # if you have Project ID, replace this
                                financial_period=period_conv,
                                financials=fin
                            )

                            st.success("Invoice generated.")
                            st.write("DOCX:", out_docx)
                            st.write("JSON metadata:", meta_json)

                            # Attempt PDF conversion
                            pdf_path = out_docx.replace(".docx", ".pdf")
                            converted, err = safe_convert_to_pdf(out_docx, pdf_path)
                            if converted:
                                st.success("PDF generated.")
                                # Provide download buttons
                                with open(pdf_path, "rb") as f:
                                    st.download_button("Download PDF", f.read(), file_name=os.path.basename(pdf_path))
                                # Attempt to preview PDF inline
                                try:
                                    with open(pdf_path, "rb") as f:
                                        base64_pdf = f.read()
                                    st.markdown("#### PDF Preview")
                                    st.write("If the PDF preview does not show, download the PDF using the button above.")
                                    st.components.v1.html(
                                        f'<iframe src="data:application/pdf;base64,{base64_pdf.encode("base64")}" width="100%" height="600px"></iframe>',
                                        height=600,
                                    )
                                except Exception:
                                    st.info("PDF preview not supported in this environment.")
                            else:
                                st.warning(f"PDF not created: {err}")
                                # Offer DOCX download
                                with open(out_docx, "rb") as f:
                                    st.download_button("Download DOCX", f.read(), file_name=os.path.basename(out_docx))

# -------------------------
# PAGE: Sessions (list existing)
# -------------------------
elif page == "Sessions":
    st.header("Existing Sessions")
    sessions = [d for d in os.listdir(BASE_DATA_PATH) if os.path.isdir(os.path.join(BASE_DATA_PATH, d))]
    if not sessions:
        st.info("No sessions found. Upload a CSV on the Home page to create one.")
    else:
        for s in sessions:
            st.write("Session:", s)
            p = os.path.join(BASE_DATA_PATH, s)
            st.write(" - files:", os.listdir(p))
            if st.button(f"Show preview {s}"):
                # show preview of CSV if exists
                csvp = os.path.join(p, "data.csv")
                if os.path.exists(csvp):
                    try:
                        df = pd.read_csv(csvp, nrows=200)
                        st.dataframe(df)
                    except Exception as e:
                        st.error(f"Failed preview: {e}")
                else:
                    st.info("No CSV in this session.")
