import re
import pandas as pd
from tableauhyperapi import HyperProcess, Connection, Telemetry, HyperException, SqlType, TableName, SchemaName
import warnings

def extract_hyper_to_excel_direct(hyper_file, hyper_filename):
    """
    Extracts data directly from a .hyper file into a dictionary of DataFrames.
    Handles multiple schemas and ensures all columns are extracted properly.
    """
    sheet_data = {}
    try:
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
                # Get all schemas in the database
                schemas = connection.catalog.get_schema_names()
                if not schemas:
                    print(f"❌ No schemas found in {hyper_file}.")
                    return sheet_data
                
                # Process each schema
                all_tables_count = 0
                for schema in schemas:
                    tables = connection.catalog.get_table_names(schema)
                    all_tables_count += len(tables)
                    
                    for table in tables:
                        # Get full table reference including schema
                        schema_name = str(table.schema_name).replace('"', '')
                        table_name_str = str(table.name).replace('"', '')
                        
                        # Clean table name for Excel sheet naming
                        clean_table_name = re.sub(r'_[A-F0-9]{32}$', '', table_name_str).replace("!", "_")
                        
                        # If we have multiple tables with the same cleaned name, add schema prefix
                        if schema_name != "Extract":
                            sheet_name = f"{schema_name}_{clean_table_name}"
                        else:
                            sheet_name = clean_table_name
                        
                        # Get column definitions
                        table_def = connection.catalog.get_table_definition(table)
                        columns = table_def.columns
                        column_names = [str(col.name).replace('"', '') for col in columns]
                        
                        # Construct query with explicit column selection to preserve order
                        column_list = ", ".join([f'"{col}"' for col in column_names])
                        query = f'SELECT {column_list} FROM "{schema_name}"."{table_name_str}"'
                        
                        # Execute query and convert to DataFrame
                        rows = connection.execute_query(query)
                        df = pd.DataFrame(rows, columns=column_names)
                        
                        if df.empty:
                            print(f"⚠ Table '{sheet_name}' is empty. Skipping...")
                            continue

                        # Attempt to convert object columns to appropriate types
                        for col in df.columns:
                            if df[col].dtype == 'object':
                                # Try datetime conversion
                                try:
                                    with warnings.catch_warnings():
                                        warnings.simplefilter("ignore", category=UserWarning)
                                        converted = pd.to_datetime(df[col], errors='coerce')
                                    if converted.notna().sum() > 0.8 * len(converted):
                                        df[col] = converted
                                except Exception:
                                    pass
                                
                                # Try numeric conversion if still object type
                                if df[col].dtype == 'object':
                                    try:
                                        numeric_vals = pd.to_numeric(df[col], errors='coerce')
                                        if numeric_vals.notna().sum() > 0.8 * len(numeric_vals):
                                            df[col] = numeric_vals
                                    except Exception:
                                        pass
                        
                        sheet_data[sheet_name] = df
                        print(f"✅ Extracted table '{sheet_name}' from {hyper_filename} with {len(df)} rows and {len(df.columns)} columns.")
                
                if all_tables_count == 0:
                    print(f"❌ No tables found in any schema in {hyper_file}.")
                
    except HyperException as e:
        print(f"❌ Hyper API error processing {hyper_file}: {e}")
    except Exception as e:
        print(f"❌ Error extracting data from {hyper_file}: {e}")
    
    return sheet_data