import pandas as pd
import os
from datetime import datetime

def read_excel_file(filepath: str) -> pd.DataFrame:
    """Read Excel or CSV file and return DataFrame."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Determine file extension
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    
    # Read file based on extension
    if ext == '.csv':
        df = pd.read_csv(filepath)
    elif ext == '.xlsx':
        df = pd.read_excel(filepath, engine='openpyxl')
    elif ext == '.xls':
        df = pd.read_excel(filepath, engine='xlrd')
    else:
        raise ValueError(f"Unsupported file extension: {ext}. Please use .csv, .xlsx, or .xls files.")
    
    # Convert column names to uppercase to match Snowflake
    df.columns = df.columns.str.upper().str.strip()
    
    return df


def get_filename_from_path(filepath: str) -> str:
    """Extract filename from full path."""
    return os.path.basename(filepath)