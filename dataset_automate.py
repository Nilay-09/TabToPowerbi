import os
from extract_twbx import extract_twbx, get_directories
from find_table_names import find_table_names
from find_hyper_files import find_hyper_files, list_tables_in_hyper
from extract_hyper_to_excel import extract_hyper_to_excel_direct
from write_to_excel import write_dataframes_to_excel

def generate_mscript_for_powerbi(excel_file, sheet_names):
    """Generates a Power BI M script for loading data from the Excel file."""
    if not sheet_names:
        return "// Error: No sheets found."
    
    excel_file_path = excel_file.replace("\\", "\\\\")
    dataset_name = "Excel_Dataset"
    selected_sheets_str = ", ".join(f'"{sheet}"' for sheet in sheet_names)
    
    mscript = f'''
let
    // Load the Excel file
    Source_{dataset_name} = Excel.Workbook(File.Contents("{excel_file_path}"), null, true),

    // Parameter for sheet selection
    SelectedSheetName = "",

    // Filter sheets of interest
    SelectedSheets = {{{selected_sheets_str}}},
    FilteredSheets = Table.SelectRows(Source_{dataset_name}, each List.Contains(SelectedSheets, [Name])),

    // Validate selected sheet exists
    TargetSheet = Table.SelectRows(FilteredSheets, each [Name] = SelectedSheetName),
    CheckSheet = if Table.IsEmpty(TargetSheet) then 
        error Error.Record(
            "Sheet not found", 
            "Available sheets: " & Text.Combine(FilteredSheets[Name], ", "), 
            [RequestedSheet = SelectedSheetName]
        )
    else TargetSheet,

    // Extract sheet data
    SheetData = try CheckSheet{{0}}[Data] otherwise error Error.Record(
        "Data extraction failed",
        "Verify sheet structure",
        [SheetName = SelectedSheetName, AvailableColumns = Table.ColumnNames(CheckSheet)]
    ),

    // Promote headers
    PromotedHeaders = Table.PromoteHeaders(SheetData, [PromoteAllScalars=true]),
    
    // Detect and apply column types dynamically
    ColumnsToTransform = Table.ColumnNames(PromotedHeaders),
    ChangedTypes = Table.TransformColumnTypes(
        PromotedHeaders,
        List.Transform(
            ColumnsToTransform,
            each {{_, 
                let
                    SampleValue = List.First(Table.Column(PromotedHeaders, _), null),
                    TypeDetect = if SampleValue = null then type text
                        else if Value.Is(SampleValue, Number.Type) then
                            if Number.Round(SampleValue) = SampleValue then Int64.Type else type number
                        else if Value.Is(SampleValue, Date.Type) then type date
                        else if Value.Is(SampleValue, DateTime.Type) then type datetime
                        else type text
                in
                    TypeDetect}}
        )
    ),

    // Clean data
    CleanedData = Table.SelectRows(ChangedTypes, each not List.Contains(Record.FieldValues(_), null)),
    FinalTable_{dataset_name} = Table.Distinct(CleanedData)
in
    FinalTable_{dataset_name}
'''
    return mscript

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

    # # Step 6: Generate Power BI M script and save it
    # mscript = generate_mscript_for_powerbi(excel_path, sheet_names)
    # with open(MSCRIPT_FILE, "w", encoding="utf-8") as file:
    #     file.write(mscript)
    # print(f"\n✅ Power BI M script saved to: {MSCRIPT_FILE}")
    
    # # Return the full Excel file path for further use if needed
    return excel_path

if __name__ == "__main__":
    twbx_file = input("🔹 Enter the path to the Tableau .twbx file: ").strip()
    if not os.path.exists(twbx_file):
        print("❌ Error: The provided .twbx file does not exist.")
    else:
        process_twbx_file(twbx_file)
