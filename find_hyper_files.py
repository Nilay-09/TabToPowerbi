import os
from extract_twbx import get_directories
from tableauhyperapi import HyperProcess, Connection, Telemetry, HyperException

def find_hyper_files():
    """Finds .hyper files inside the extracted directory."""
    _, _, EXTRACT_DIR = get_directories()
    hyper_files = {}
    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.endswith('.hyper'):
                hyper_files[file] = os.path.join(root, file)
    return hyper_files

def list_tables_in_hyper(hyper_file):
    """Lists all tables inside a .hyper file across all schemas."""
    try:
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
                # Get all schemas in the database
                schemas = connection.catalog.get_schema_names()
                if not schemas:
                    print(f"⚠ No schemas found in {hyper_file}.")
                    return []
                
                table_list = []
                for schema in schemas:
                    tables = connection.catalog.get_table_names(schema)
                    if tables:
                        for table in tables:
                            table_info = {
                                'schema': str(table.schema_name),
                                'name': str(table.name),
                                'full_name': f"{table.schema_name}.{table.name}"
                            }
                            
                            # Get column information
                            try:
                                table_def = connection.catalog.get_table_definition(table)
                                columns = table_def.columns
                                table_info['columns'] = [
                                    {
                                        'name': str(col.name).replace('"', ''),
                                        'type': str(col.type)
                                    }
                                    for col in columns
                                ]
                                table_info['column_count'] = len(columns)
                            except Exception as e:
                                print(f"⚠ Error getting columns for {table.name}: {e}")
                                table_info['columns'] = []
                                table_info['column_count'] = 0
                            
                            table_list.append(table_info)
                            print(f"🔍 Found table '{table.schema_name}.{table.name}' with {table_info['column_count']} columns")
                
                if not table_list:
                    for schema in schemas:
                        print(f"⚠ No tables found in schema '{schema}'.")
                
                return table_list
                
    except HyperException as e:
        print(f"❌ Hyper API error processing {hyper_file}: {e}")
    except Exception as e:
        print(f"❌ Error extracting table names from {hyper_file}: {e}")
    return []