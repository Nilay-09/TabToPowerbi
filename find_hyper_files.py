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
    """Lists all tables inside a .hyper file in the 'Extract' schema only."""
    try:
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(endpoint=hyper.endpoint, database=hyper_file) as connection:
                schema_name = "Extract"
                tables = connection.catalog.get_table_names(schema_name)
                if not tables:
                    print(f"⚠ No tables found in {hyper_file} under schema '{schema_name}'.")
                    return []
                table_list = [table.name for table in tables]
                return table_list
    except HyperException as e:
        print(f"❌ Hyper API error processing {hyper_file}: {e}")
    except Exception as e:
        print(f"❌ Error extracting table names from {hyper_file}: {e}")
    return []
