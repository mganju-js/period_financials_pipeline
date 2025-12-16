import pandas as pd
import os
from datetime import datetime

def read_excel_file(filepath: str) -> pd.DataFrame:
    """Read Excel file and return DataFrame."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    # Read Excel file
    df = pd.read_excel(filepath)
    
    # Convert column names to uppercase to match Snowflake
    df.columns = df.columns.str.upper().str.strip()
    
    return df


def get_filename_from_path(filepath: str) -> str:
    """Extract filename from full path."""
    return os.path.basename(filepath)