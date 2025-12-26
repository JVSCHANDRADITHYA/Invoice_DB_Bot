import os
import json
import hashlib
import datetime
from typing import overload
import duckdb

from rapidfuzz import process, fuzz
from docxtpl import DocxTemplate

class Invoicer:
    def __init__(self, session_id, base_path="Data/sessions", invoice_path="Invoices"):
        self.session_id = session_id
        self.base_path = base_path
        self.invoice_path = invoice_path
        
        os.makedirs(invoice_path, exist_ok=True)

        self.db_path = os.path.join(base_path, session_id, "duckdb.duckdb")
        self.conn = duckdb.connect(self.db_path)
        
    # -------------------------------------------------------------
    #  MATCHING HELPERS
    # -------------------------------------------------------------
    def fuzzy_match_top(self, column, value, limit=3):
        """Return top-N fuzzy matches for a column."""
        rows = self.conn.execute(f"SELECT DISTINCT {column} FROM sample_table").fetchall()
        values = [r[0] for r in rows if r[0] is not None]

        matches = process.extract(
            value,
            values,
            scorer=fuzz.WRatio,
            limit=limit
        )

        # matches is [(match_value, score, index), ...]
        return [{"match": m[0], "score": m[1]} for m in matches]


    # -------------------------------------------------------------
    #   1) MATCH RESOURCE
    # -------------------------------------------------------------
    def match_resource(self, input_name_or_id):
        """Return top 3 matches across both Name and ID."""
        name_matches = self.fuzzy_match_top('"Resource Name"', input_name_or_id)
        id_matches = self.fuzzy_match_top('"Resource ID"', input_name_or_id)

        # Combine & sort by score
        combined = [
            {"type": "name", "match": m["match"], "score": m["score"]}
            for m in name_matches
        ] + [
            {"type": "id", "match": m["match"], "score": m["score"]}
            for m in id_matches
        ]

        combined_sorted = sorted(combined, key=lambda x: x["score"], reverse=True)
        return combined_sorted[:3]


    # -------------------------------------------------------------
    #   2) MATCH PROJECT
    # -------------------------------------------------------------
    def match_project(self, input_name_or_id):
        """Return top 3 matches across both Name and ID."""
        name_matches = self.fuzzy_match_top('"Project Name"', input_name_or_id)
        id_matches = self.fuzzy_match_top('"Project ID"', input_name_or_id)

        combined = [
            {"type": "name", "match": m["match"], "score": m["score"]}
            for m in name_matches
        ] + [
            {"type": "id", "match": m["match"], "score": m["score"]}
            for m in id_matches
        ]

        combined_sorted = sorted(combined, key=lambda x: x["score"], reverse=True)
        return combined_sorted[:3]


    # -------------------------------------------------------------
    #   3) CONVERT MONTH NAME TO YYYY-MM (Financial Period)
    # -------------------------------------------------------------
    def convert_period(self, text):
        text = text.lower().strip()

        # Map month names
        months = {
            "january": "01", "february": "02", "march": "03",
            "april": "04", "may": "05", "june": "06",
            "july": "07", "august": "08", "september": "09",
            "october": "10", "november": "11", "december": "12"
        }

        for m in months:
            if m in text:
                mm = months[m]
                yyyy = str(datetime.date.today().year)
                return f"{yyyy}-{mm}"

        # If exact YYYY-MM provided
        if "-" in text:
            return text

        raise ValueError("Cannot interpret financial period from input.")

    # -------------------------------------------------------------
    #  4) COMPUTE HOURS + RATE + TOTAL COST
    # -------------------------------------------------------------
    def compute_financials(self, resource_name, project_name, financial_period):
        q = f"""
        SELECT 
            SUM("Posted Hours") AS total_hours,
            ANY_VALUE("Resource Rate") AS rate
        FROM sample_table
        WHERE "Resource Name" = '{resource_name}'
          AND "Project Name" = '{project_name}'
          AND "Financial Period (Posted Date)" = '{financial_period}';
        """
        
        row = self.conn.execute(q).fetchone()

        if not row or row[0] is None:
            return {"hours": 0, "rate": 0, "amount": 0}

        hours = row[0]
        rate = row[1]
        amount = hours * rate
        return {"hours": hours, "rate": rate, "amount": amount}

    # -------------------------------------------------------------
    #  5) MAKE HASHES FOR IRN + ACK
    # -------------------------------------------------------------
    def make_hash(self, text):
        return hashlib.sha256(text.encode()).hexdigest()[:12]

    # -------------------------------------------------------------
    #  6) GENERATE INVOICE USING DOCXTPL
    # -------------------------------------------------------------
    def generate_invoice(self, resource_name, project_name, project_id,
                         financial_period, financials, path=None):

        today = datetime.date.today().strftime("%d-%b-%Y")
        if path is not None:
            folder_path = path
            os.makedirs(folder_path, exist_ok=True)
        else:
            folder_name = f"{resource_name}_{project_id}".replace(" ", "_")
            folder_path = os.path.join(self.invoice_path, folder_name)
            os.makedirs(folder_path, exist_ok=True)

        context = {
            "irn": self.make_hash(resource_name + project_name),
            "ack": self.make_hash(project_id),
            "ack_date": today,
            "invoice_no": f"INV-{self.session_id}-{self.make_hash(project_id)[:4]}",
            "invoice_date": today,
            "Del_note": "",
            "mode_terms": "Online/Immediate",
            "ref_no": "",
            "ref_date": today,
            "others_ref": "N/A",

            # Shipping fields
            "consignee_ship_to": f"{project_name} ({project_id})",
            "buy_ord_no": project_id,
            "dated_2": today,
            "dispatch_doc_no": "",
            "del_note_date": today,
            "dispatch_thr": "Courier",
            "destination": "Hyderabad",
            "buyer_bill_to": f"{project_name} ({project_id})",
            "country": "India",
            "terms_of_delivery": "FOB",

            # Critical fields
            "Project_Name": project_name,
            "resource_Name": resource_name,
            "no_of_hours": financials["hours"],
            "resource_rate": financials["rate"],
            "total_amount": financials["amount"],
            "HSN_SAC": "9983",
            "Quantity": financials["hours"],
            "rate": financials["rate"],
            "Amount": financials["amount"],

            # Table rows example (optional)
            "table_rows": []
        }

        # Save metadata JSON
        json_path = os.path.join(folder_path, "invoice.json")
        with open(json_path, "w") as f:
            json.dump(context, f, indent=4)

        # Render DOCX
        template = DocxTemplate("invoice.docx")
        template.render(context)
        output_path = os.path.join(folder_path, "invoice.docx")
        template.save(output_path)

        return output_path, json_path
    def generate_all_invoices(self, resource_name, project_name, financial_period):
        """Generate invoices for all matching projects for a resource"""
        # Get project ID
        q = f"""
        SELECT DISTINCT "Project ID"
        FROM sample_table
        WHERE "Resource Name" = '{resource_name};'
        """
        rows = self.conn.execute(q).fetchall()
        project_ids = [r[0] for r in rows]

        generated_files = []

        for pid in project_ids:
            financials = self.compute_financials(resource_name, project_name, financial_period)
            invoice_path, json_path = self.generate_invoice(
                resource_name, project_name, pid,
                financial_period, financials
            )
            generated_files.append({
                "project_id": pid,
                "invoice_path": invoice_path,
                "json_path": json_path
            })
        return generated_files
    
    def all_resources_invoice(self, financial_period):
        """Generate invoices for all resources in the database for a given period"""
        q = f"""
        SELECT "Resource Name", "Project Name", "Project ID" AS total_hours
        FROM sample_table
        WHERE "Financial Period (Posted Date)" = '2025-11'
        GROUP BY "Resource Name", "Project Name", "Project ID"
        HAVING SUM("Posted Hours") > 0;

        """
        # 'If you're actually reading this, you're one hell of a depressed individual. Congrats.'
        # what you need is 'auto-erotic mummification' - Dr. Vince Masuka
        rows = self.conn.execute(q).fetchall()

        generated_files = []

        for row in rows:
            resource_name, project_name, project_id= row
            financials = self.compute_financials(resource_name, project_name, financial_period)
            invoice_path, json_path = self.generate_invoice(
                resource_name, project_name, project_id,
                financial_period, financials
            )
            generated_files.append({
                "resource_name": resource_name,
                "project_id": project_id,
                "invoice_path": invoice_path,
                "json_path": json_path
            })
        return generated_files
    
    def project_invoice_with_all_resources(self, project_id, financial_period):
        """
        Generate ONE invoice for a project.
        Invoice contains multiple rows (one per resource).
        """

        # 1. Fetch all resource-level financials for the project
        q = f"""
        SELECT
            "Resource Name",
            SUM("Posted Hours") AS hours,
            AVG("Resource Rate") AS rate,
            SUM("Posted Hours" * "Resource Rate") AS amount
        FROM sample_table
        WHERE "Project ID" = '{project_id}'
        AND "Financial Period (Posted Date)" = '{financial_period}'
        GROUP BY "Resource Name"
        HAVING SUM("Posted Hours") > 0;
        """

        rows = self.conn.execute(q).fetchall()

        if not rows:
            raise ValueError("No billable data for this project & period")

        # 2. Get project name once
        project_name = self.conn.execute(
            f"""
            SELECT DISTINCT "Project Name"
            FROM sample_table
            WHERE "Project ID" = '{project_id}'
            """
        ).fetchone()[0]
        
        resource_names, hours_list, rates_list, amounts_list = zip(*rows)
        
        resource_names = '\n'.join(resource_names)
        financials = {
            "hours": sum(hours_list),
            "rate": 0,  # Not applicable for multi-resource invoice
            "amount": sum(amounts_list)
        }
        
        # generate invoice
        path, json_path = self.generate_invoice(
            resource_name=resource_names,
            project_name=project_name,
            project_id=project_id,
            financial_period=financial_period,
            financials=financials,
            path=f'project_invoices/{project_id}'

        )


#usage:
inv = Invoicer(session_id="master")
# use project_invoice_with_all_resources
invoices = inv.project_invoice_with_all_resources(project_id="91HYFY25_RASESI_NOA", financial_period="2025-11")

# example usage:
# invoicer = Invoicer(session_id="abc123")
# matches = invoicer.match_resource("John Doe")