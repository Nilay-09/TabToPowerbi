import os
import re
import pandas as pd
from extract_twbx import extract_twbx, get_directories
from find_table_names import find_table_names
from find_hyper_files import find_hyper_files, list_tables_in_hyper
from extract_hyper_to_excel import extract_hyper_to_excel_direct
from write_to_excel import write_dataframes_to_excel


try:
    from tableauhyperapi import Timestamp as HyperTimestamp
except ImportError:
    HyperTimestamp = ()



def preprocess_formula(formula):
    """
    1) Collapse whitespace  
    2) TODAY() → NOW() → pd.Timestamp("today")  
    3) Ensure INDEX() becomes INDEX(row)
    """
    if not formula:
        return ""
    # collapse all runs of whitespace
    formula = " ".join(formula.split())
    # Tableau TODAY() → NOW() → pandas Timestamp
    formula = re.sub(r'TODAY\(\)', 'NOW()', formula, flags=re.IGNORECASE)
    formula = re.sub(r'NOW\(\)', 'pd.Timestamp("today")', formula, flags=re.IGNORECASE)
    # make INDEX() call pass the row
    formula = re.sub(r'\bINDEX\(\)', 'INDEX(row)', formula, flags=re.IGNORECASE)
    return formula


def transform_if_then_else(formula):
    """
    Turn IF … THEN … ELSEIF … THEN … ELSE … END
    into nested Python ternaries.
    """
    # normalize keywords
    f = re.sub(r'\belseif\b', 'ELSEIF', formula, flags=re.IGNORECASE)
    f = re.sub(r'\bif\b',     'IF',     f, flags=re.IGNORECASE)
    f = re.sub(r'\bthen\b',   'THEN',   f, flags=re.IGNORECASE)
    f = re.sub(r'\belse\b',   'ELSE',   f, flags=re.IGNORECASE)
    f = re.sub(r'\bend\b',    'END',    f, flags=re.IGNORECASE)

    # capture the IF, any ELSEIFs, optional ELSE, then END
    pattern = re.compile(
        r'IF\s+(.+?)\s+THEN\s+(.+?)'               # IF cond THEN expr
        r'(?:\s+ELSEIF\s+(.+?)\s+THEN\s+(.+?))*'   # 0+ ELSEIF blocks
        r'(?:\s+ELSE\s+(.+?))?'                    # optional ELSE
        r'\s+END',                                 # END
        flags=re.DOTALL
    )
    m = pattern.search(f)
    if not m:
        return formula

    # first IF
    conds = [(m.group(1).strip(), m.group(2).strip())]
    # then any ELSEIFs
    elseif_matches = re.findall(
        r'ELSEIF\s+(.+?)\s+THEN\s+(.+?)(?=\s+ELSEIF|\s+ELSE|\s+END)',
        f, flags=re.DOTALL
    )
    for c, e in elseif_matches:
        conds.append((c.strip(), e.strip()))
    # final ELSE (if present)
    final_else = m.group(5).strip() if m.group(5) else "None"

    # build nested Python expression
    expr = final_else
    for cond, res in reversed(conds):
        expr = f"({res} if {cond} else {expr})"
    return expr



# helpers to compare possibly-mixed datetime types
def LT(a, b):
    return pd.to_datetime(str(a), errors='coerce') <  pd.to_datetime(str(b), errors='coerce')

def LTE(a, b):
    return pd.to_datetime(str(a), errors='coerce') <= pd.to_datetime(str(b), errors='coerce')

def GT(a, b):
    return pd.to_datetime(str(a), errors='coerce') >  pd.to_datetime(str(b), errors='coerce')

def GTE(a, b):
    return pd.to_datetime(str(a), errors='coerce') >= pd.to_datetime(str(b), errors='coerce')


def DATEDIFF(part, start, end):
    """
    Works even if start/end are tableauhyperapi.Timestamp.
    """
    # coerce any HyperTimestamp (or string) into pandas.Timestamp
    start = pd.to_datetime(str(start), errors='coerce')
    end   = pd.to_datetime(str(end),   errors='coerce')

    p = part.lower()
    if p == 'year':
        return end.year - start.year
    if p == 'month':
        return (end.year - start.year) * 12 + (end.month - start.month)
    if p == 'day':
        return (end - start).days
    if p == 'hour':
        return (end - start).total_seconds() / 3600
    return None


def DATEPART(part, dt):
    """
    Works even if dt is tableauhyperapi.Timestamp.
    """
    # coerce into pandas.Timestamp
    dt = pd.to_datetime(str(dt), errors='coerce')
    p = part.lower()
    if p == 'year':    return dt.year
    if p == 'month':   return dt.month
    if p == 'day':     return dt.day
    if p == 'quarter': return (dt.month - 1) // 3 + 1
    if p == 'weekday': return dt.dayofweek
    return None


def apply_tableau_formula(df, formula, field_name):
    try:
        # 1) normalize TODAY()/INDEX()
        formula = preprocess_formula(formula)

        # 2) handle IF/ELSEIF/ELSE/END
        if "IF" in formula and "THEN" in formula and "END" in formula:
            formula = transform_if_then_else(formula)

        # 3) turn [Field] into row["Field"]
        formula = translate_tableau_formula(formula)

        # 4) wrap any direct date comparisons against "today"
        formula = re.sub(
            r'row\["([^"]+)"\]\s*<\s*pd\.Timestamp\("today"\)',
            r'LT(row["\1"], pd.Timestamp("today"))',
            formula
        )
        formula = re.sub(
            r'row\["([^"]+)"\]\s*<=\s*pd\.Timestamp\("today"\)',
            r'LTE(row["\1"], pd.Timestamp("today"))',
            formula
        )
        formula = re.sub(
            r'row\["([^"]+)"\]\s*>\s*pd\.Timestamp\("today"\)',
            r'GT(row["\1"], pd.Timestamp("today"))',
            formula
        )
        formula = re.sub(
            r'row\["([^"]+)"\]\s*>=\s*pd\.Timestamp\("today"\)',
            r'GTE(row["\1"], pd.Timestamp("today"))',
            formula
        )

        # 5) literal min/max → number
        formula = re.sub(r'min\(([-0-9\.]+)\)', r'\1', formula, flags=re.IGNORECASE)
        formula = re.sub(r'max\(([-0-9\.]+)\)', r'\1', formula, flags=re.IGNORECASE)

        # empty formula → blank column
        if not formula.strip():
            df[field_name] = ""
            return True

        safe_globals = {
            "pd": pd,
            "DATEDIFF": DATEDIFF,
            "DATEPART": DATEPART,
            "ISNULL": ISNULL,
            "INDEX": lambda row: row.name + 1,
            "LT": LT,
            "LTE": LTE,
            "GT": GT,
            "GTE": GTE,
        }

        # 6) apply row‑by‑row
        df[field_name] = df.apply(lambda row: eval(formula, safe_globals, {"row": row}), axis=1)
        return True

    except Exception as e:
        print(f"  ❌ Error applying formula '{formula}' to field '{field_name}': {e}")
        return False


def translate_tableau_formula(formula):
    return re.sub(r'\[([^\]]+)\]', r'row["\1"]', formula)


def ISNULL(x):
    return pd.isna(x)

def INDEX(row_index):
    return row_index + 1







def ensure_unique_column_names(df):
    """
    Ensure all column names are unique (case-insensitive) by adding suffixes to duplicates.
    
    Args:
        df: DataFrame with potential duplicate column names.
    Returns:
        DataFrame with unique column names.
    """
    seen = {}
    new_columns = []
    for col in df.columns:
        base = col.lower()
        if base not in seen:
            seen[base] = 1
            new_columns.append(col)
        else:
            new_name = f"{col}_calc"
            # Check case-insensitively if new_name already exists
            while new_name.lower() in seen:
                seen[base] += 1
                new_name = f"{col}_calc{seen[base]}"
            seen[new_name.lower()] = 1
            print(f"  ⚠️ Renamed duplicate column '{col}' to '{new_name}'")
            new_columns.append(new_name)
    df.columns = new_columns
    return df


def process_twbx_file(twbx_file):
    """Orchestrates the full process from extraction to Excel with calculated fields."""
    BASE_DIR, OUTPUT_DIR, _ = get_directories()
    MSCRIPT_FILE = os.path.join(OUTPUT_DIR, "powerbi_mscript.txt")
    CALC_FIELDS_FILE = os.path.join(OUTPUT_DIR, "calculated_fields.txt")

    # Step 1: Extract the .twbx file
    extract_twbx(twbx_file)

    # Step 2: Extract dataset names, table names, and calculated fields
    table_mapping, table_names, calculated_fields = find_table_names()
    # Identify and skip pure parameter fields (they just echo the parameter)
    param_fields = set(calculated_fields.get("Parameters", {}).keys())

    with open(CALC_FIELDS_FILE, 'w', encoding='utf-8') as f:
        f.write("# Tableau Calculated Fields\n\n")
        for datasource, fields in calculated_fields.items():
            f.write(f"## Datasource: {datasource}\n\n")
            for field_name, details in fields.items():
                f.write(f"### {field_name}\n")
                f.write(f"Formula: {details['formula']}\n")
                f.write(f"Type: {details.get('datatype', 'Unknown')}\n\n")
    print(f"✅ Saved calculated field definitions to {CALC_FIELDS_FILE}")

    # Step 3: Find .hyper files
    hyper_files = find_hyper_files()
    if not hyper_files:
        print(f"❌ No .hyper files found in {twbx_file}. Skipping extraction...")
        return
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
        if hyper_filename in table_mapping:
            mapped_name = table_mapping[hyper_filename]
            if "Extract" in sheet_data:
                sheet_data[mapped_name] = sheet_data.pop("Extract")
            else:
                sheet_data = {mapped_name: df for _, df in sheet_data.items()}
        combined_sheet_data.update(sheet_data)
    if not combined_sheet_data:
        print("❌ No data extracted from any .hyper file.")
        return

    # Step 5: Apply calculated fields where applicable, skipping parameter-only fields
    calculated_field_stats = {"applied": 0, "failed": 0, "total": 0}
    for sheet_name, df in combined_sheet_data.items():
        # find matching datasource
        matching_ds = None
        for ds in calculated_fields:
            if ds == sheet_name or ds in sheet_name or sheet_name in ds:
                matching_ds = ds
                break
        if not matching_ds:
            continue

        # filter out pure parameter fields
        fields_to_apply = {
            fn: details
            for fn, details in calculated_fields[matching_ds].items()
            if fn not in param_fields
        }
        if not fields_to_apply:
            continue

        print(f"\n📊 Applying calculated fields to '{sheet_name}' (matched with '{matching_ds}')")
        applied_fields = set()
        max_iterations = 3

        for iteration in range(max_iterations):
            applied_in_iteration = 0
            for field_name, details in fields_to_apply.items():
                if field_name in applied_fields:
                    continue
                calculated_field_stats["total"] += 1
                formula = details["formula"]
                required = re.findall(r'\[([^\]]+)\]', formula)
                if not required or all(col in df.columns for col in required):
                    print(f"  🔄 Applying '{field_name}' calculation (iteration {iteration+1})")
                    success = apply_tableau_formula(df, formula, field_name)
                    if success:
                        applied_fields.add(field_name)
                        applied_in_iteration += 1
                        calculated_field_stats["applied"] += 1
                        print(f"  ✅ Successfully applied '{field_name}' calculation")
                    else:
                        calculated_field_stats["failed"] += 1
                        print(f"  ⚠️ Could not apply '{field_name}' calculation")
            if applied_in_iteration == 0:
                break

        unapplied = set(fields_to_apply) - applied_fields
        if unapplied:
            print(f"\n⚠️ Could not apply {len(unapplied)} calculated fields:")
            for f in unapplied:
                print(f"  - {f}")

    print(f"\n📊 Calculated fields summary:")
    print(f"  - Total: {calculated_field_stats['total']}")
    print(f"  - Applied: {calculated_field_stats['applied']}")
    print(f"  - Failed: {calculated_field_stats['failed']}")

    # Step 5.5: Ensure unique column names (case-insensitive)
    for name, df in combined_sheet_data.items():
        if name != 'Column_Metadata':
            combined_sheet_data[name] = ensure_unique_column_names(df)

    # Step 6: Create a metadata sheet
    column_metadata = []
    for name, df in combined_sheet_data.items():
        for col in df.columns:
            is_calc = any(col in fields for fields in calculated_fields.values())
            formula_text = ""
            for fields in calculated_fields.values():
                if col in fields:
                    formula_text = fields[col]['formula']
                    break
            column_metadata.append({
                'Sheet': name,
                'Column': col,
                'Data Type': str(df[col].dtype),
                'Sample Value': str(df[col].iloc[0]) if not df.empty else '',
                'Is Calculated': 'Yes' if is_calc else 'No',
                'Formula': formula_text
            })
    metadata_df = pd.DataFrame(column_metadata)
    combined_sheet_data['Column_Metadata'] = metadata_df

    # Step 7: Write to Excel
    base_name = os.path.splitext(os.path.basename(twbx_file))[0]
    excel_path = os.path.join(OUTPUT_DIR, f"{base_name}.xlsx")
    sheet_names = write_dataframes_to_excel(combined_sheet_data, excel_path)
    print(f"\n✅ All data combined into {excel_path} with {len(sheet_names)} sheets.")

    # Step 8: Write column summary text file
    total_cols = sum(len(df.columns) for name, df in combined_sheet_data.items() if name != 'Column_Metadata')
    summary_path = os.path.join(OUTPUT_DIR, f"{base_name}_column_summary.txt")
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write(f"# Column Summary for {base_name}\n\n")
        f.write(f"Total columns extracted: {total_cols}\n\n")
        for name, df in combined_sheet_data.items():
            if name == 'Column_Metadata':
                continue
            f.write(f"## Sheet: {name}\n\n")
            f.write(f"Total columns: {len(df.columns)}\n\n")
            for i, col in enumerate(df.columns, 1):
                is_calc = any(col in fields for fields in calculated_fields.values())
                f.write(f"{i}. {col} ({df[col].dtype}) - Calculated: {'Yes' if is_calc else 'No'}\n")
            f.write("\n")
    print(f"✅ Column summary written to {summary_path}")

    # Print out sheet details
    for name, df in combined_sheet_data.items():
        if name == 'Column_Metadata':
            continue
        print(f"\n📋 Sheet '{name}' column details:")
        for i, col in enumerate(df.columns, 1):
            is_calc = any(col in fields for fields in calculated_fields.values())
            print(f"  {i}. {col} ({df[col].dtype}) - Calculated: {'Yes' if is_calc else 'No'}")

    return excel_path
