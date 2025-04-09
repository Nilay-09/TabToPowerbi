# import re
# import pandas as pd
# from openpyxl import load_workbook

# def write_dataframes_to_excel(sheet_data, excel_file):
#     """
#     Writes a dictionary of DataFrames to an Excel workbook.
#     Applies date formatting ('yyyy-mm-dd') for datetime columns.
#     """
#     with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
#         sheet_names = []
#         for sheet_name, df in sheet_data.items():
#             # Sanitize the sheet name (max 31 chars, remove invalid characters)
#             safe_sheet_name = re.sub(r'[\/:*?"<>|]', '_', sheet_name)[:31]
#             base_name = safe_sheet_name
#             counter = 1
#             while safe_sheet_name in sheet_names:
#                 suffix = f"_{counter}"
#                 safe_sheet_name = base_name[:31 - len(suffix)] + suffix
#                 counter += 1
#             sheet_names.append(safe_sheet_name)
#             df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
#             print(f"✅ Written sheet '{safe_sheet_name}' with {len(df)} rows to Excel.")
#     # Adjust Excel cell formatting for date columns using openpyxl
#     wb = load_workbook(excel_file)
#     for sheet_name in wb.sheetnames:
#         ws = wb[sheet_name]
#         header = [cell.value for cell in ws[1]]
#         for col_idx, col_name in enumerate(header, start=1):
#             if ws.max_row > 1:
#                 cell_value = ws.cell(row=2, column=col_idx).value
#                 if isinstance(cell_value, (pd.Timestamp, str)):
#                     try:
#                         pd.to_datetime(cell_value, errors='raise')
#                         for row in range(2, ws.max_row + 1):
#                             ws.cell(row=row, column=col_idx).number_format = 'yyyy-mm-dd'
#                     except Exception:
#                         pass
#     wb.save(excel_file)
#     print(f"✅ Data written and formatted in Excel file: {excel_file}")
#     return sheet_names




import pandas as pd
import os

def write_dataframes_to_excel(dataframes_dict, output_path):
    """
    Writes multiple DataFrames to a single Excel file with improved formatting.
    
    Args:
        dataframes_dict: Dictionary mapping sheet_name to DataFrame.
        output_path: Path where the Excel file will be saved.
        
    Returns:
        List of sheet names that were written.
    """
    # Check if any data exists
    if not dataframes_dict:
        print("❌ No data to write to Excel.")
        return []
    
    # Create a Pandas Excel writer using XlsxWriter as the engine
    try:
        writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
        sheet_names = []
        
        for sheet_name, df in dataframes_dict.items():
            # Excel sheet names have a 31 character limit
            # Replace invalid characters and truncate if necessary
            safe_sheet_name = str(sheet_name).replace('/', '_').replace('\\', '_').replace('*', '_').replace('?', '_').replace('[', '_').replace(']', '_').replace(':', '_')
            if len(safe_sheet_name) > 31:
                safe_sheet_name = safe_sheet_name[:30] + '~'
            
            # Check for duplicate sheet names
            if safe_sheet_name in sheet_names:
                # Add a suffix to make it unique
                base_name = safe_sheet_name[:27] if len(safe_sheet_name) > 27 else safe_sheet_name
                suffix = 1
                while f"{base_name}_{suffix}" in sheet_names:
                    suffix += 1
                safe_sheet_name = f"{base_name}_{suffix}"
            
            # Write the DataFrame to Excel
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            sheet_names.append(safe_sheet_name)
            
            # Get the xlsxwriter workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets[safe_sheet_name]
            
            # Add a header format
            header_format = workbook.add_format({
                'bold': True,
                'fg_color': '#D7E4BC',
                'border': 1
            })
            
            # Format the header row
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-fit columns
            for col_num, col in enumerate(df.columns):
                # Find maximum length of column data
                max_len = max(
                    df[col].astype(str).map(len).max(),
                    len(str(col))
                ) + 2  # Add a little extra space
                
                # Set column width to a maximum of 50 characters
                worksheet.set_column(col_num, col_num, min(max_len, 50))
            
            print(f"✅ Written sheet '{safe_sheet_name}' with {len(df)} rows and {len(df.columns)} columns to Excel.")
        
        # Save the Excel file
        writer.close()
        print(f"✅ Data written and formatted in Excel file: {output_path}")
        return sheet_names
        
    except Exception as e:
        print(f"❌ Error writing to Excel: {e}")
        return []