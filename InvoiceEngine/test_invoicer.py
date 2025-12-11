from Invoicer import Invoicer
from resolve_helper import resolve_best_match   # put function into helper file or same file

session_id = "ca4d640a"
inv = Invoicer(session_id)

print("\n========= RESOURCE MATCHING =========")
resource_input = input("Enter Resource Name or Resource ID: ")

res_candidates = inv.match_resource(resource_input)
resource_name = resolve_best_match(res_candidates, "Resource")

if resource_name is None:
    exit()


print("\n========= PROJECT MATCHING =========")
project_input = input("Enter Project Name or Project ID: ")

proj_candidates = inv.match_project(project_input)
project_name = resolve_best_match(proj_candidates, "Project")

if project_name is None:
    exit()


print("\n========= FINANCIAL PERIOD =========")
period_input = input("Enter Financial Period: ")

financial_period = inv.convert_period(period_input)

financials = inv.compute_financials(resource_name, project_name, financial_period)

print("\nHour Summary:")
print(financials)

confirm = input("\nGenerate invoice? (yes/no): ").strip().lower()
if confirm != "yes":
    print("Aborted.")
    exit()

output, meta = inv.generate_invoice(
    resource_name,
    project_name,
    project_name,  # assuming project_name is unique
    financial_period,
    financials
)

print("\nInvoice generated at:", output)
print("Metadata saved at:", meta)
