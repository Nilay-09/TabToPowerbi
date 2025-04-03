import re
import pandas as pd
from tableauhyperapi import HyperProcess, Connection, Telemetry, HyperException

def extract_hyper_to_excel_direct(hyper_file, hyper_filename):
    """
    Extracts data directly from a .hyper file into a dictionary of DataFrames.
    For each table, attempts to convert columns to datetime if possible.
    """
    sheet_data = {}
    try:
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
                schema_name = "Extract"
                tables = connection.catalog.get_table_names(schema_name)
                if not tables:
                    print(f"❌ No tables found in {hyper_file}.")
                    return sheet_data
                for table in tables:
                    table_name_str = str(table.name).replace('"', '')
                    # Remove potential hash suffixes and sanitize sheet name
                    clean_table_name = re.sub(r'_[A-F0-9]{32}$', '', table_name_str).replace("!", "_")
                    columns = connection.catalog.get_table_definition(table).columns
                    column_names = [str(col.name).replace('"', '') for col in columns]
                    query = f"SELECT * FROM {table}"
                    rows = connection.execute_query(query)
                    df = pd.DataFrame(rows, columns=column_names)
                    if df.empty:
                        print(f"⚠ Table {clean_table_name} is empty. Skipping...")
                        continue

                    # Attempt to convert object columns to datetime when possible
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            try:
                                converted = pd.to_datetime(df[col], errors='raise', infer_datetime_format=True)
                                if converted.notna().sum() > 0.8 * len(converted):
                                    df[col] = converted
                            except Exception:
                                pass
                    sheet_data[clean_table_name] = df
                    print(f"✅ Extracted table '{clean_table_name}' from {hyper_filename} with {len(df)} rows.")
    except HyperException as e:
        print(f"❌ Hyper API error processing {hyper_file}: {e}")
    except Exception as e:
        print(f"❌ Error extracting data from {hyper_file}: {e}")
    return sheet_data
