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

import os
import pythoncom
from win32com import client

def safe_convert_to_pdf(input_docx, output_pdf):
    try:
        pythoncom.CoInitialize()

        # Convert to absolute paths
        input_path = os.path.abspath(input_docx)
        output_path = os.path.abspath(output_pdf)

        if not os.path.exists(input_path):
            return False, f"File not found: {input_path}"

        word = client.Dispatch("Word.Application")
        word.visible = False

        doc = word.Documents.Open(input_path)
        doc.SaveAs(output_path, FileFormat=17)  # PDF = 17
        doc.Close()
        word.Quit()

        return True, None

    except Exception as e:
        return False, str(e)

    finally:
        try:
            pythoncom.CoUninitialize()
        except:
            pass



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
# elif page == "Invoice Generator":
#     st.header("Invoice Generator")

#     sessions = [d for d in os.listdir(BASE_DATA_PATH) if os.path.isdir(os.path.join(BASE_DATA_PATH, d))]
#     sessions.sort(reverse=True)

#     chosen_session = st.selectbox("Select session", ["-- new session --"] + sessions, key="session_select")

#     if chosen_session == "-- new session --":
#         st.info("Upload a CSV on Home page to create a session first.")
#         st.stop()

#     sid = chosen_session
#     session_path = os.path.join(BASE_DATA_PATH, sid)

#     # Load DB
#     try:
#         import duckdb
#         conn = duckdb.connect(os.path.join(session_path, "duckdb.duckdb"))
#         full_resources = [r[0] for r in conn.execute('SELECT DISTINCT "Resource Name" FROM sample_table').fetchall() if r[0]]
#         conn.close()
#     except Exception as e:
#         st.error(f"DB error: {e}")
#         st.stop()

#     st.subheader("Select Resource")

#     # initialize state
#     if "selected_resource" not in st.session_state:
#         st.session_state.selected_resource = "-- choose --"

#     resource_choice = st.selectbox(
#         "Select Resource",
#         ["-- choose --"] + full_resources,
#         key="selected_resource"
#     )

#     resource_fuzzy = st.text_input("Or type a fuzzy resource name", key="resource_fuzzy")

#     # Resolve resource
#     final_resource = None

#     if st.session_state.selected_resource != "-- choose --":
#         final_resource = st.session_state.selected_resource
#     elif resource_fuzzy:
#         from rapidfuzz import process, fuzz
#         best = process.extractOne(resource_fuzzy, full_resources, scorer=fuzz.WRatio)
#         if best:
#             final_resource = best[0]
#             st.success(f"Fuzzy matched Resource → {final_resource}  (score={best[1]})")

#     if not final_resource:
#         st.stop()


#     # Filter projects for ONLY that resource
#     try:
#         conn = duckdb.connect(os.path.join(session_path, "duckdb.duckdb"))
#         filtered_projects = [
#             r[0] for r in conn.execute(f'''
#                 SELECT DISTINCT "Project Name"
#                 FROM sample_table
#                 WHERE "Resource Name" = '{final_resource}'
#             ''').fetchall() if r[0]
#         ]
#         conn.close()
#     except Exception as e:
#         st.error(f"Cannot retrieve projects for resource: {e}")
#         st.stop()

#     st.subheader("Select Project")

#     project_choice = st.selectbox("Select Project", ["-- choose --"] + filtered_projects, key="project_select")
#     project_fuzzy = st.text_input("Or type fuzzy project name", key="project_fuzzy")

#     final_project = None
#     if project_choice != "-- choose --":
#         final_project = project_choice
#     elif project_fuzzy:
#         from rapidfuzz import process, fuzz
#         best = process.extractOne(project_fuzzy, filtered_projects, scorer=fuzz.WRatio)
#         if best:
#             final_project = best[0]
#             st.success(f"Fuzzy matched Project → {final_project}  (score={best[1]})")

#     if not final_project:
#         st.stop()

#     # Get Project ID from DB (MISSING BEFORE!)
#     conn = duckdb.connect(os.path.join(session_path, "duckdb.duckdb"))
#     project_id = conn.execute(f'''
#         SELECT DISTINCT "Project ID"
#         FROM sample_table
#         WHERE "Project Name" = '{final_project}'
#     ''').fetchone()[0]
#     conn.close()

#     st.subheader("Financial Period")
#     period_input = st.text_input("Enter Financial Period (e.g. November or 2025-11)", key="period_input")

#     if st.button("Compute Financials", key="compute_financials"):
#         invoicer = Invoicer(session_id=sid, base_path=BASE_DATA_PATH, invoice_path=INVOICES_PATH)

#         try:
#             period_conv = invoicer.convert_period(period_input)
#         except Exception as e:
#             st.error(f"Period error: {e}")
#             st.stop()

#         fin = invoicer.compute_financials(final_resource, final_project, period_conv)

#         st.subheader("Preview Financials")
#         st.json(fin)

#         # Store in Streamlit state
#         st.session_state["inv_fin"] = fin
#         st.session_state["inv_period"] = period_conv
#         st.session_state["inv_resource"] = final_resource
#         st.session_state["inv_project"] = final_project
#         st.session_state["inv_project_id"] = project_id

#     # Generate invoice
#     if "inv_fin" in st.session_state:
#         if st.button("Generate Invoice", key="generate_invoice"):
#             invoicer = Invoicer(session_id=sid, base_path=BASE_DATA_PATH, invoice_path=INVOICES_PATH)

#             with st.spinner("Generating invoice..."):
#                 out_docx, meta_json = invoicer.generate_invoice(
#                     resource_name=st.session_state["inv_resource"],
#                     project_name=st.session_state["inv_project"],
#                     project_id=st.session_state["inv_project_id"],
#                     financial_period=st.session_state["inv_period"],
#                     financials=st.session_state["inv_fin"]
#                 )

#             st.success("Invoice generated.")
#             st.write("DOCX file saved at:", out_docx)

#             # Download DOCX
#             with open(out_docx, "rb") as f:
#                 st.download_button("Download DOCX", f.read(), file_name=os.path.basename(out_docx))

#             # Convert to PDF
#             pdf_path = out_docx.replace(".docx", ".pdf")
#             converted, err = safe_convert_to_pdf(out_docx, pdf_path)

#             if converted:
#                 st.success("PDF generated.")
#                 with open(pdf_path, "rb") as f:
#                     pdf_data = f.read()
#                     st.download_button("Download PDF", pdf_data, file_name=os.path.basename(pdf_path))
#             else:
#                 st.warning(f"PDF conversion failed: {err}")
# -------------------------
# PAGE: Invoice Generator
# -------------------------
elif page == "Invoice Generator":
    import base64

    st.header("Invoice Generator")

    # -------------------------
    # Session Selection
    # -------------------------
    sessions = [
        d for d in os.listdir(BASE_DATA_PATH)
        if os.path.isdir(os.path.join(BASE_DATA_PATH, d))
    ]
    sessions.sort(reverse=True)

    chosen_session = st.selectbox(
        "Select session",
        ["-- new session --"] + sessions,
        key="session_select"
    )

    if chosen_session == "-- new session --":
        st.info("Upload a CSV on Home page to create a session first.")
        st.stop()

    sid = chosen_session
    session_path = os.path.join(BASE_DATA_PATH, sid)

    # -------------------------
    # Load DB + Resources
    # -------------------------
    try:
        import duckdb
        conn = duckdb.connect(os.path.abspath(os.path.join(session_path, "duckdb.duckdb")))
        full_resources = [
            r[0]
            for r in conn.execute(
                'SELECT DISTINCT "Resource Name" FROM sample_table'
            ).fetchall()
            if r[0]
        ]
        conn.close()
    except Exception as e:
        st.error(f"DB error: {e}")
        st.stop()

    st.subheader("Select Resource")

    resource_choice = st.selectbox(
        "Select Resource",
        ["-- choose --"] + full_resources,
        key="resource_choice"
    )

    resource_fuzzy = st.text_input("Or type a fuzzy resource name", key="resource_fuzzy")

    # -------------------------
    # Resolve Resource
    # -------------------------
    final_resource = None
    if resource_choice != "-- choose --":
        final_resource = resource_choice
    elif resource_fuzzy:
        from rapidfuzz import process, fuzz

        best = process.extractOne(resource_fuzzy, full_resources, scorer=fuzz.WRatio)
        if best:
            final_resource = best[0]
            st.success(f"Fuzzy matched Resource → {final_resource} (score={best[1]})")

    if not final_resource:
        st.stop()

    # -------------------------
    # Load Projects associated with selected resource
    # -------------------------
    try:
        conn = duckdb.connect(os.path.abspath(os.path.join(session_path, "duckdb.duckdb")))
        filtered_projects = [
            r[0]
            for r in conn.execute(
                f'''
                SELECT DISTINCT "Project Name"
                FROM sample_table
                WHERE "Resource Name" = '{final_resource}'
                '''
            ).fetchall()
            if r[0]
        ]
        conn.close()
    except Exception as e:
        st.error(f"Cannot retrieve projects for resource: {e}")
        st.stop()

    st.subheader("Select Project")

    project_choice = st.selectbox(
        "Select Project",
        ["-- choose --"] + filtered_projects,
        key="project_choice"
    )

    project_fuzzy = st.text_input("Or type fuzzy project name", key="project_fuzzy")

    final_project = None
    if project_choice != "-- choose --":
        final_project = project_choice
    elif project_fuzzy:
        from rapidfuzz import process, fuzz

        best = process.extractOne(project_fuzzy, filtered_projects, scorer=fuzz.WRatio)
        if best:
            final_project = best[0]
            st.success(f"Fuzzy matched Project → {final_project} (score={best[1]})")

    if not final_project:
        st.stop()

    # -------------------------
    # Retrieve Project ID
    # -------------------------
    conn = duckdb.connect(os.path.abspath(os.path.join(session_path, "duckdb.duckdb")))
    project_id = conn.execute(
        f'''
        SELECT DISTINCT "Project ID"
        FROM sample_table
        WHERE "Project Name" = '{final_project}'
        '''
    ).fetchone()[0]
    conn.close()

    # -------------------------
    # Financial Period
    # -------------------------
    st.subheader("Financial Period")
    period_input = st.text_input(
        "Enter Financial Period (e.g. November or 2025-11)",
        key="period_input"
    )

    # -------------------------
    # Compute Financials
    # -------------------------
    if st.button("Compute Financials", key="compute_financials"):
        invoicer = Invoicer(session_id=sid, base_path=BASE_DATA_PATH, invoice_path=INVOICES_PATH)

        try:
            period_conv = invoicer.convert_period(period_input)
        except Exception as e:
            st.error(f"Period error: {e}")
            st.stop()

        fin = invoicer.compute_financials(final_resource, final_project, period_conv)

        st.subheader("Preview Financials")
        st.json(fin)

        # Save state
        st.session_state["inv_fin"] = fin
        st.session_state["inv_period"] = period_conv
        st.session_state["inv_resource"] = final_resource
        st.session_state["inv_project"] = final_project
        st.session_state["inv_project_id"] = project_id

    # -------------------------
    # Generate Invoice
    # -------------------------
    if "inv_fin" in st.session_state:
        if st.button("Generate Invoice", key="generate_invoice"):
            invoicer = Invoicer(
                session_id=sid,
                base_path=BASE_DATA_PATH,
                invoice_path=INVOICES_PATH
            )

            with st.spinner("Generating invoice..."):
                out_docx, meta_json = invoicer.generate_invoice(
                    resource_name=st.session_state["inv_resource"],
                    project_name=st.session_state["inv_project"],
                    project_id=st.session_state["inv_project_id"],
                    financial_period=st.session_state["inv_period"],
                    financials=st.session_state["inv_fin"]
                )

            st.success("Invoice generated.")
            st.write("DOCX file saved at:", out_docx)

            # DOCX Download
            with open(out_docx, "rb") as f:
                st.download_button(
                    "Download DOCX",
                    f.read(),
                    file_name=os.path.basename(out_docx)
                )

            # -------------------------
            # Convert to PDF
            # -------------------------
            pdf_path = out_docx.replace(".docx", ".pdf")
            converted, err = safe_convert_to_pdf(out_docx, pdf_path)

            if not converted:
                st.warning(f"PDF conversion failed: {err}")
                st.stop()

            st.success("PDF generated!")

            with open(pdf_path, "rb") as f:
                pdf_data = f.read()

            st.download_button(
                "Download PDF",
                pdf_data,
                file_name=os.path.basename(pdf_path)
            )

            # -------------------------
            # PDF Preview
            # -------------------------
            try:
                base64_pdf = base64.b64encode(pdf_data).decode("utf-8")
                st.markdown("### Invoice Preview")

                pdf_display = f"""
                    <iframe
                        src="data:application/pdf;base64,{base64_pdf}"
                        width="100%" height="700px">
                    </iframe>
                """

                st.components.v1.html(pdf_display, height=1200 , width=1200)

            except Exception as e:
                st.error(f"PDF preview failed: {e}")

            

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
