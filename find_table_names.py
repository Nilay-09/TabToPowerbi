import os
import re
import xml.etree.ElementTree as ET
from extract_twbx import get_directories

def find_table_names():
    """
    Extracts dataset names, table names, and calculated fields from .twb files.
    
    Returns:
        table_mapping: dict mapping .hyper filenames to datasource captions.
        table_names: dict mapping table names to datasource captions.
        calculated_fields: dict containing calculated field definitions for each datasource.
    """
    _, _, EXTRACT_DIR = get_directories()
    table_mapping = {}
    table_names = {}
    calculated_fields = {}
    
    # Register XML namespaces if needed
    namespaces = {
        'tableau': 'http://www.tableausoftware.com/xml/tableau',
    }
    
    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.endswith('.twb'):
                file_path = os.path.join(root, file)
                try:
                    # Try reading the file as text to preserve all information
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # Parse XML directly
                    try:
                        tree = ET.fromstring(content)
                    except ET.ParseError:
                        # Fall back to regular parsing if fromstring fails
                        tree = ET.parse(file_path).getroot()
                    
                    # Process each datasource
                    for datasource in tree.findall(".//datasource"):
                        caption = datasource.get("caption", "").strip()
                        name = datasource.get("name", "").strip()
                        
                        # Use caption if available, otherwise use name
                        source_identifier = caption if caption else name
                        if not source_identifier:
                            continue
                        
                        # Find connection information for .hyper files
                        for connection in datasource.findall(".//connection"):
                            dbname = connection.get("dbname", "").strip()
                            if dbname and dbname.endswith(".hyper"):
                                hyper_filename = os.path.basename(dbname)
                                table_mapping[hyper_filename] = source_identifier
                                print(f"✅ Mapped hyper file '{hyper_filename}' to '{source_identifier}'")
                        
                        # Extract column information
                        for column in datasource.findall(".//column"):
                            col_name = column.get("name", "").strip()
                            col_caption = column.get("caption", "").strip() or col_name
                            
                            if not col_name:
                                continue
                                
                            # Check if it's a calculated field by looking for a calculation element
                            calculation = column.find(".//calculation")
                            if calculation is not None:
                                formula = calculation.get("formula", "").strip()
                                datatype = column.get("datatype", "").strip()
                                
                                # Store the calculated field information
                                if source_identifier not in calculated_fields:
                                    calculated_fields[source_identifier] = {}
                                
                                calculated_fields[source_identifier][col_caption] = {
                                    'name': col_name,
                                    'formula': formula,
                                    'datatype': datatype
                                }
                                print(f"✅ Found calculated field '{col_caption}' in '{source_identifier}'")
                            
                        # Find table relations
                        for relation in datasource.findall(".//relation"):
                            tname = relation.get("name", "").strip()
                            if tname:
                                table_names[tname] = source_identifier
                                print(f"✅ Found relation '{tname}' in '{source_identifier}'")
                    
                    # Also look for parameters (they might be needed for calculations)
                    for param in tree.findall(".//parameter"):
                        param_name = param.get("name", "").strip()
                        caption = param.get("caption", "").strip() or param_name
                        
                        # Find which datasource this parameter belongs to
                        for ds in tree.findall(".//datasource"):
                            ds_caption = ds.get("caption", "").strip()
                            ds_name = ds.get("name", "").strip()
                            ds_id = ds_caption if ds_caption else ds_name
                            
                            if param.get("datasource") == ds_name:
                                if ds_id not in calculated_fields:
                                    calculated_fields[ds_id] = {}
                                
                                # Store the parameter as a special type of calculated field
                                calculated_fields[ds_id][caption] = {
                                    'name': param_name,
                                    'formula': f"PARAMETER({param_name})",
                                    'datatype': param.get("datatype", "").strip(),
                                    'is_parameter': True
                                }
                                print(f"✅ Found parameter '{caption}' in '{ds_id}'")
                
                except Exception as e:
                    print(f"❌ Error processing {file_path}: {e}")
    
    # Summary of what was found
    print(f"\n📊 Found {len(table_mapping)} hyper file mappings.")
    print(f"📊 Found {len(table_names)} table relations.")
    print(f"📊 Found {sum(len(fields) for fields in calculated_fields.values())} calculated fields across {len(calculated_fields)} datasources.")
    
    return table_mapping, table_names, calculated_fields