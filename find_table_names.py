import os
import xml.etree.ElementTree as ET
from extract_twbx import get_directories


def find_table_names():
    """
    Extracts dataset names and table names from the .twb files in the extracted directory.
    Returns:
        table_mapping: dict mapping .hyper filenames to datasource captions.
        table_names: dict mapping table names to datasource captions.
    """
    _, _, EXTRACT_DIR = get_directories()
    table_mapping = {}
    table_names = {}
    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.endswith('.twb'):
                file_path = os.path.join(root, file)
                try:
                    tree = ET.parse(file_path)
                    xml_root = tree.getroot()
                    for datasource in xml_root.findall(".//datasource"):
                        caption = datasource.get("caption", "").strip()
                        for connection in datasource.findall(".//connection"):
                            dbname = connection.get("dbname", "").strip()
                            if dbname and dbname.endswith(".hyper"):
                                hyper_filename = os.path.basename(dbname)
                                table_mapping[hyper_filename] = caption
                        for relation in datasource.findall(".//relation"):
                            tname = relation.get("name", "").strip()
                            if tname:
                                table_names[tname] = caption
                except ET.ParseError as e:
                    print(f"❌ XML Parsing Error in {file_path}: {e}")
                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")
    return table_mapping, table_names
