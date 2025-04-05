import os
import pandas as pd
import pyodbc
from dataset_automate import process_twbx_file


import warnings
import pandas as pd




# Connection helper using context managers
def get_connection():
    conn_str = (
        r"Driver={ODBC Driver 18 for SQL Server};"
        r"Server=tcp:decision.database.windows.net,1433;"
        r"Database=finalDecision;"
        r"Uid=priyank;"
        r"Pwd=530228@mka;"
        r"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)


def auto_convert_column(series, threshold=0.8):
    """Convert series to datetime if most values can be converted."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        converted = pd.to_datetime(series, errors='coerce')
    return converted if converted.notna().sum() >= threshold * len(converted) else series

def map_dtype(series):
    """Map a pandas series to a SQL Server data type."""
    if pd.api.types.is_integer_dtype(series.dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(series.dtype):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(series.dtype):
        return "DATE"
    else:
        return "NVARCHAR(255)"

def encode_sheet_name(sheet_name):
    """Encode the sheet name as a reversible hexadecimal string."""
    return ''.join(format(ord(c), '02X') for c in sheet_name)

def decode_sheet_name(encoded):
    """Decode the hexadecimal string back to the original sheet name."""
    return ''.join(chr(int(encoded[i:i+2], 16)) for i in range(0, len(encoded), 2))

def create_table_and_insert_data(excel_file_path):
    """Load Excel data and insert into SQL Server."""
    if not os.path.exists(excel_file_path):
        print(f"❌ Error: Excel file not found at {excel_file_path}")
        return

    xls = pd.ExcelFile(excel_file_path)
    base_name = os.path.splitext(os.path.basename(excel_file_path))[0]

    with get_connection() as conn:
        with conn.cursor() as cursor:
            for sheet_name in xls.sheet_names:
                # Encode the sheet name to preserve the original name reversibly.
                encoded_sheet_name = encode_sheet_name(sheet_name)
                table_name = f"{base_name}_{encoded_sheet_name}"

                # Read the sheet into a DataFrame.
                df = pd.read_excel(xls, sheet_name=sheet_name)

                # Attempt automatic conversion for columns with object dtype.
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = auto_convert_column(df[col])

                # Build the CREATE TABLE SQL statement.
                columns = [f"[{col}] {map_dtype(df[col])}" for col in df.columns]
                create_table_sql = f"CREATE TABLE [{table_name}] (\n  {',\n  '.join(columns)}\n);"

                print(f"Creating table: {table_name}")
                print("Create Table SQL:")
                print(create_table_sql)
                cursor.execute(create_table_sql)
                conn.commit()

                # Prepare the INSERT statement.
                columns_list = df.columns.tolist()
                placeholders = ", ".join("?" * len(columns_list))
                insert_sql = f"INSERT INTO {table_name} ({', '.join(f'[{col}]' for col in columns_list)}) VALUES ({placeholders})"

                # Enable fast_executemany if available (e.g., with pyodbc).
                cursor.fast_executemany = True

                # Batch insert using executemany.
                batch_size = 10000
                data_batch = []
                for i, (_, row) in enumerate(df.iterrows(), start=1):
                    row_values = tuple(None if pd.isna(val) else val for val in row)
                    data_batch.append(row_values)
                    if i % batch_size == 0:
                        cursor.executemany(insert_sql, data_batch)
                        conn.commit()
                        data_batch = []

                # Insert any remaining rows.
                if data_batch:
                    cursor.executemany(insert_sql, data_batch)
                    conn.commit()

                print(f"Data inserted successfully into table: {table_name}\n")


def get_all_table_names():
    """Fetch all table names from the SQL Server database."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = [row[0] for row in cursor.fetchall()]
    return tables

def get_filtered_decoded_table_names(excel_path):
    """
    Retrieve and filter table names from the database based on the TWBX file's base name,
    remove the prefix, and decode the remaining hexadecimal string.
    Returns a dictionary mapping the original table name to the decoded name.
    """
    base_name = os.path.splitext(os.path.basename(excel_path))[0]
    mapping = {}
    for table in get_all_table_names():
        if table.startswith(base_name + "_"):
            encoded_part = table[len(base_name) + 1:]
            mapping[table] = decode_sheet_name(encoded_part)
    return mapping

def rename_tables(rename_mapping):
    """
    Rename SQL tables using sp_rename so that the table name becomes the decoded name.
    rename_mapping should be a dictionary mapping {original_table_name: decoded_name}.
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for original, decoded in rename_mapping.items():
                rename_sql = f"EXEC sp_rename '{original}', '{decoded}'"
                print(f"Renaming table: {original} --> {decoded}")
                cursor.execute(rename_sql)
                conn.commit()

def generate_mscript_for_sql(server_name, database_name, selected_tables):
    """
    Generate a Power BI M script for loading data from SQL Server.
    """
    selected_tables_str = ", ".join(f'"{table}"' for table in selected_tables)
    mscript = f'''
let
    // Define the SQL Server connection parameters
    ServerName = "{server_name}",
    DatabaseName = "{database_name}",

    // Connect to SQL Server
    Source_SQL = Sql.Database(ServerName, DatabaseName),

    // Parameter for table selection
    SelectedTableName = "",

    // List of tables of interest
    SelectedTables = {{{selected_tables_str}}},
    FilteredTables = Table.SelectRows(Source_SQL, each List.Contains(SelectedTables, [Name])),

    // Validate selected table exists
    TargetTable = Table.SelectRows(FilteredTables, each [Name] = SelectedTableName),
    CheckTable = if Table.IsEmpty(TargetTable) then 
        error Error.Record(
            "Table not found", 
            "Available tables: " & Text.Combine(FilteredTables[Name], ", "), 
            [RequestedTable = SelectedTableName]
        )
    else TargetTable,

    // Extract table data
    TableData = try Sql.Database(ServerName, DatabaseName, [Query="SELECT * FROM [" & SelectedTableName & "]"])
        otherwise error Error.Record(
            "Data extraction failed",
            "Verify table structure",
            [TableName = SelectedTableName]
        ),

    // Detect and apply column types dynamically
    ColumnsToTransform = Table.ColumnNames(TableData),
    ChangedTypes = Table.TransformColumnTypes(
        TableData,
        List.Transform(
            ColumnsToTransform,
            each {{_, 
                let
                    SampleValue = List.First(Table.Column(TableData, _), null),
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
    FinalTable_SQL = Table.Distinct(CleanedData)
in
    FinalTable_SQL
'''
    return mscript

if __name__ == '__main__':
    twbx_file = input("🔹 Enter the path to the Tableau .twbx file: ").strip()
    if not os.path.exists(twbx_file):
        print("❌ Error: The provided .twbx file does not exist.")
    else:

        excel_path = process_twbx_file(twbx_file)
        create_table_and_insert_data(excel_path)
        

        table_mapping = get_filtered_decoded_table_names(excel_path)
        rename_tables(table_mapping)
        

        SERVER_NAME = "decision.database.windows.net"
        DATABASE_NAME = "finalDecision"
        SELECTED_TABLES = list(table_mapping.values())
        mscript = generate_mscript_for_sql(SERVER_NAME, DATABASE_NAME, SELECTED_TABLES)
        MSCRIPT_FILE = os.path.join(os.path.dirname(excel_path), "powerbi_mscript_sql.txt")
        with open(MSCRIPT_FILE, "w", encoding="utf-8") as file:
            file.write(mscript)
        print(f"\n✅ Power BI M script (SQL version) saved to: {MSCRIPT_FILE}")
    
