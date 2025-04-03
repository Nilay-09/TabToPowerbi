# import zipfile
# import json
# import os
# import pandas as pd
# import re

# def generate_mscript_for_powerbi(excel_file_path, selected_sheet_name):
#     if not os.path.exists(excel_file_path):
#         return f"// Error: File not found - {excel_file_path}"

#     xls = pd.ExcelFile(excel_file_path)
#     escaped_file_path = excel_file_path.replace("\\", "\\\\")
#     sheet_names = xls.sheet_names
#     sheet_names_str = ", ".join(f'"{name}"' for name in sheet_names)

#     file_key = re.sub(r'\W+', '', os.path.basename(excel_file_path).replace(".", ""))

#     mscript = f'''
#     // Load the Excel file
#     Source_{file_key} = Excel.Workbook(File.Contents("{escaped_file_path}"), null, true),

#     // Parameter for sheet selection
#     SelectedSheetName = "{selected_sheet_name}",

#     // Filter sheets of interest
#     SelectedSheets = {{{sheet_names_str}}},
#     FilteredSheets = Table.SelectRows(Source_{file_key}, each List.Contains(SelectedSheets, [Name])),

#     // Validate selected sheet exists
#     TargetSheet = Table.SelectRows(FilteredSheets, each [Name] = SelectedSheetName),
#     CheckSheet = if Table.IsEmpty(TargetSheet) then 
#         error Error.Record(
#             "Sheet not found", 
#             "Available sheets: " & Text.Combine(FilteredSheets[Name], ", "), 
#             [RequestedSheet = SelectedSheetName]
#         )
#     else TargetSheet,

#     // Extract sheet data
#     SheetData = try CheckSheet{{0}}[Data] otherwise error Error.Record(
#         "Data extraction failed",
#         "Verify sheet structure",
#         [SheetName = SelectedSheetName, AvailableColumns = Table.ColumnNames(CheckSheet)]
#     ),

#     // Promote headers
#     PromotedHeaders = Table.PromoteHeaders(SheetData, [PromoteAllScalars=true]),
    
#     // Detect and apply column types dynamically
#     ColumnsToTransform = Table.ColumnNames(PromotedHeaders),
#     ChangedTypes = Table.TransformColumnTypes(
#         PromotedHeaders,
#         List.Transform(
#             ColumnsToTransform,
#             each {{_, 
#                 let
#                     SampleValue = List.First(Table.Column(PromotedHeaders, _), null),
#                     TypeDetect = if SampleValue = null then type text
#                         else if Value.Is(SampleValue, Number.Type) then
#                             if Number.Round(SampleValue) = SampleValue then Int64.Type else type number
#                         else if Value.Is(SampleValue, Date.Type) then type date
#                         else if Value.Is(SampleValue, DateTime.Type) then type datetime
#                         else type text
#                 in
#                     TypeDetect}}
#         )
#     ),

#     // Clean data
#     CleanedData = Table.SelectRows(ChangedTypes, each not List.Contains(Record.FieldValues(_), null)),
#     FinalTable_{file_key} = Table.Distinct(CleanedData)
#     '''
#     return file_key, mscript

# def process_tfl_file(tfl_path):
#     if not os.path.exists(tfl_path):
#         print("Invalid file path. Please check the file location and try again.")
#         return

#     extract_folder = "extracted_flow"
#     os.makedirs(extract_folder, exist_ok=True)

#     def extract_tfl(tfl_path, extract_to):
#         try:
#             with zipfile.ZipFile(tfl_path, 'r') as zip_ref:
#                 zip_ref.extractall(extract_to)
#         except zipfile.BadZipFile:
#             print("Error: Invalid ZIP archive.")
#             return None
#         return extract_to

#     extracted_folder = extract_tfl(tfl_path, extract_folder)
#     if not extracted_folder:
#         return

#     flow_file_path = os.path.join(extracted_folder, "flow")
#     if not os.path.exists(flow_file_path):
#         print("Flow file not found.")
#         return

#     with open(flow_file_path, "r", encoding="utf-8") as file:
#         flow_data = json.load(file)

#     print("TFL file extracted and flow data loaded successfully!")
    
#     def generate_m_script(flow_data):
#         connections = flow_data.get("connections", {})
#         m_queries = []

#         for conn_name, conn_data in connections.items():
#             file_path = conn_data.get("connectionAttributes", {}).get("filename", "")
#             selected_sheet_name = conn_data.get("selectedSheet", "")
            
#             if not file_path or not os.path.exists(file_path):
#                 print(f"File not found: {file_path}")
#                 continue
            
#             file_key, sheet_script = generate_mscript_for_powerbi(file_path, selected_sheet_name)
            
#             if sheet_script and not sheet_script.startswith("// Error"):
#                 m_queries.append(sheet_script)

#         return "let\n" + "\n".join(m_queries) + "\nin\n    FinalTable_VideoGameSalesxlsx"

#     m_script = generate_m_script(flow_data)

#     output_file = "power_query_script.m"
#     with open(output_file, "w", encoding="utf-8") as f:
#         f.write(m_script)

#     print(f"\nM script saved as: {output_file}")

# if __name__ == "__main__":
#     tfl_path = r"D:\TabToPowerbi\Superstore Sales Analysis.tfl"
#     process_tfl_file(tfl_path)











import zipfile
import json
import os
import pandas as pd
import re

def generate_mscript_for_powerbi(excel_file_path, selected_sheet_name):
    if not os.path.exists(excel_file_path):
        return f"// Error: File not found - {excel_file_path}"

    xls = pd.ExcelFile(excel_file_path)
    escaped_file_path = excel_file_path.replace("\\", "\\\\")
    sheet_names = xls.sheet_names
    sheet_names_str = ", ".join(f'"{name}"' for name in sheet_names)

    file_key = re.sub(r'\W+', '', os.path.basename(excel_file_path).replace(".", ""))

    mscript = f'''
    // Load the Excel file
    Source_{file_key} = Excel.Workbook(File.Contents("{escaped_file_path}"), null, true),

    // Parameter for sheet selection
    SelectedSheetName = "{selected_sheet_name}",

    // Filter sheets of interest
    SelectedSheets = {{{sheet_names_str}}},
    FilteredSheets = Table.SelectRows(Source_{file_key}, each List.Contains(SelectedSheets, [Name])),

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
    FinalTable_{file_key} = Table.Distinct(CleanedData)
    '''
    return file_key, mscript

def generate_sql_mscript_for_powerbi(server, database, table_or_query):
    # This function generates an M script for a SQL Server connection.
    # You can adjust the schema, table name, or use a native query as needed.
    mscript = f'''
    // Load data from SQL Server
    Source_SQL = Sql.Database("{server}", "{database}"),
    // Adjust the schema and table name below as necessary.
    TargetData = Source_SQL{{[Schema="dbo",Item="{table_or_query}"]}},
    FinalTable_SQL = Table.Distinct(TargetData)
    '''
    return mscript

def process_tfl_file(tfl_path):
    if not os.path.exists(tfl_path):
        print("Invalid file path. Please check the file location and try again.")
        return

    extract_folder = "extracted_flow"
    os.makedirs(extract_folder, exist_ok=True)

    def extract_tfl(tfl_path, extract_to):
        try:
            with zipfile.ZipFile(tfl_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        except zipfile.BadZipFile:
            print("Error: Invalid ZIP archive.")
            return None
        return extract_to

    extracted_folder = extract_tfl(tfl_path, extract_folder)
    if not extracted_folder:
        return

    flow_file_path = os.path.join(extracted_folder, "flow")
    if not os.path.exists(flow_file_path):
        print("Flow file not found.")
        return

    with open(flow_file_path, "r", encoding="utf-8") as file:
        flow_data = json.load(file)

    print("TFL file extracted and flow data loaded successfully!")
    
    def generate_m_script(flow_data):
        connections = flow_data.get("connections", {})
        m_queries = []

        for conn_name, conn_data in connections.items():
            connection_attributes = conn_data.get("connectionAttributes", {})

            # Debugging: Print connection details
            print(f"\nProcessing connection: {conn_name}")
            print(json.dumps(connection_attributes, indent=4))  # Print connection attributes for debugging

            file_path = connection_attributes.get("filename", "")
            selected_sheet_name = conn_data.get("selectedSheet", "")

            # Check for Excel file connection
            if file_path:
                if not os.path.exists(file_path):
                    print(f"File not found: {file_path}")
                    continue
                file_key, sheet_script = generate_mscript_for_powerbi(file_path, selected_sheet_name)
                if sheet_script and not sheet_script.startswith("// Error"):
                    m_queries.append(sheet_script)

            # Check for SQL Server connection
            elif connection_attributes.get("server") and connection_attributes.get("database"):
                server = connection_attributes.get("server")
                database = connection_attributes.get("database")
                table_or_query = connection_attributes.get("table", "YourDefaultTable")

                sql_script = generate_sql_mscript_for_powerbi(server, database, table_or_query)
                m_queries.append(sql_script)

            else:
                print(f"⚠️ Unsupported connection type in connection: {conn_name}")
                continue

        return "let\n" + "\n".join(m_queries) + "\nin\n    FinalTable"

    
    m_script = generate_m_script(flow_data)

    output_file = "power_query_script.m"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(m_script)

    print(f"\nM script saved as: {output_file}")

if __name__ == "__main__":
    # Provide the path to your TFL file here.
    tfl_path = r"D:\TabToPowerbi\databysql.tfl"  # e.g., r"C:\path\to\your\tfl_file.tfl"
    process_tfl_file(tfl_path)
