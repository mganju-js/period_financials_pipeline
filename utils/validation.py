import pandas as pd
from typing import Tuple, List
import config

def validate_dataframe(df: pd.DataFrame, filename: str) -> Tuple[bool, List[str]]:
    """
    Validate the input dataframe before loading to Snowflake.
    Returns: (is_valid, list_of_errors)
    """
    errors = []
    
    # 1. Check required columns exist
    missing_cols = set(config.REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return False, errors
    
    # 2. Check for null values in key columns
    for col in config.KEY_COLUMNS:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            errors.append(f"Found {null_count} null values in key column: {col}")
    
    # 3. Validate YEAR range
    invalid_years = df[~df['YEAR'].between(config.MIN_YEAR, config.MAX_YEAR)]
    if len(invalid_years) > 0:
        errors.append(f"Found {len(invalid_years)} rows with invalid YEAR (must be {config.MIN_YEAR}-{config.MAX_YEAR})")
    
    # 4. Validate PERIOD range
    invalid_periods = df[~df['PERIOD'].between(config.MIN_PERIOD, config.MAX_PERIOD)]
    if len(invalid_periods) > 0:
        errors.append(f"Found {len(invalid_periods)} rows with invalid PERIOD (must be {config.MIN_PERIOD}-{config.MAX_PERIOD})")
    
    # 5. Check data types for financial columns
    numeric_cols = config.FINANCIAL_COLUMNS
    for col in numeric_cols:
        if col in df.columns:
            try:
                pd.to_numeric(df[col], errors='coerce')
            except:
                errors.append(f"Column {col} contains non-numeric values")
    
    # 6. Check for duplicate keys
    duplicates = df[df.duplicated(subset=config.KEY_COLUMNS, keep=False)]
    if len(duplicates) > 0:
        errors.append(f"Found {len(duplicates)} duplicate rows based on YEAR, PERIOD, STORE_LOCATION")
    
    # 7. Basic row count check
    if len(df) == 0:
        errors.append("DataFrame is empty")
    
    is_valid = len(errors) == 0
    
    return is_valid, errors


def print_validation_summary(df: pd.DataFrame):
    """Print a summary of the data for review."""
    print("\n" + "="*60)
    print("DATA VALIDATION SUMMARY")
    print("="*60)
    print(f"Total Rows: {len(df)}")
    print(f"Unique Stores: {df['STORE_LOCATION'].nunique()}")
    print(f"Year Range: {df['YEAR'].min()} - {df['YEAR'].max()}")
    print(f"Period Range: {df['PERIOD'].min()} - {df['PERIOD'].max()}")
    print(f"\nSample of data:")
    print(df[['YEAR', 'PERIOD', 'STORE_LOCATION', 'SALES', 'COGS']].head())
    print("="*60 + "\n")