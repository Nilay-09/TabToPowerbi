import os
from extract_twbx import extract_twbx, get_directories
from find_table_names import find_table_names
from find_hyper_files import find_hyper_files, list_tables_in_hyper
from extract_hyper_to_excel import extract_hyper_to_excel_direct
from write_to_excel import write_dataframes_to_excel


def process_twbx_file(twbx_file):
    """Orchestrates the full process from extraction to M script generation."""
    BASE_DIR, OUTPUT_DIR, _ = get_directories()
    MSCRIPT_FILE = os.path.join(OUTPUT_DIR, "powerbi_mscript.txt")

    # Step 1: Extract .twbx file
    extract_twbx(twbx_file)

    # Step 2: Extract dataset names & table names from .twb files
    table_mapping, table_names = find_table_names()

    # Step 3: Find .hyper files
    hyper_files = find_hyper_files()
    if not hyper_files:
        print(f"❌ No .hyper files found in {twbx_file}. Skipping extraction...")
        return

    # Optional: list tables for logging
    all_tables = []
    for hyper_filename, hyper_file_path in hyper_files.items():
        tables = list_tables_in_hyper(hyper_file_path)
        all_tables.extend(tables)
    print("\n📊 Extracted Table Names from .hyper files:")
    for table in all_tables:
        print(f" - {table}")

    # Step 4: Extract data from each .hyper file
    combined_sheet_data = {}
    for hyper_filename, hyper_file_path in hyper_files.items():
        sheet_data = extract_hyper_to_excel_direct(hyper_file_path, hyper_filename)
        combined_sheet_data.update(sheet_data)

    if not combined_sheet_data:
        print("❌ No data extracted from any .hyper file.")
        return

    # Step 5: Write all DataFrames to a single Excel file
    base_name = os.path.splitext(os.path.basename(twbx_file))[0]
    excel_path = os.path.join(OUTPUT_DIR, f"{base_name}.xlsx")
    sheet_names = write_dataframes_to_excel(combined_sheet_data, excel_path)
    print(f"\n✅ All data combined into {excel_path} with {len(sheet_names)} sheets.")

    return excel_path

if __name__ == "__main__":
    twbx_file = input("🔹 Enter the path to the Tableau .twbx file: ").strip()
    if not os.path.exists(twbx_file):
        print("❌ Error: The provided .twbx file does not exist.")
    else:
        process_twbx_file(twbx_file)
