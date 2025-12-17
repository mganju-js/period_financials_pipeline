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
    
    # 2. Convert YEAR to numeric
    try:
        df['YEAR'] = pd.to_numeric(df['YEAR'], errors='coerce')
    except Exception as e:
        errors.append(f"Error converting YEAR to numeric: {e}")
        return False, errors
    
    # 3. Convert PERIOD - handle string formats like "P1", "P01", "Period 1", etc.
    try:
        # First convert to string to handle any format
        df['PERIOD'] = df['PERIOD'].astype(str)
        
        # Extract numeric part (handles "P1", "P01", "Period 1", etc.)
        df['PERIOD'] = df['PERIOD'].str.extract('(\d+)', expand=False)
        
        # Convert to numeric
        df['PERIOD'] = pd.to_numeric(df['PERIOD'], errors='coerce')
    except Exception as e:
        errors.append(f"Error converting PERIOD to numeric: {e}")
        return False, errors
    
    # 4. Check for null values in key columns
    for col in config.KEY_COLUMNS:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            errors.append(f"Found {null_count} null values in key column: {col}")
            # Show some examples of the nulls
            null_rows = df[df[col].isnull()][['YEAR', 'PERIOD', 'STORE_LOCATION']].head(3)
            errors.append(f"Sample rows with null {col}:\n{null_rows.to_string()}")
    
    # 5. Validate YEAR range (after converting to numeric)
    valid_years = df['YEAR'].notna()
    invalid_years = df[valid_years & ~df['YEAR'].between(config.MIN_YEAR, config.MAX_YEAR)]
    if len(invalid_years) > 0:
        errors.append(f"Found {len(invalid_years)} rows with invalid YEAR (must be {config.MIN_YEAR}-{config.MAX_YEAR})")
        sample_years = df.loc[invalid_years.index[:3], ['YEAR', 'PERIOD', 'STORE_LOCATION']]
        errors.append(f"Sample invalid rows:\n{sample_years.to_string()}")
    
    # 6. Validate PERIOD range (after converting to numeric)
    valid_periods = df['PERIOD'].notna()
    invalid_periods = df[valid_periods & ~df['PERIOD'].between(config.MIN_PERIOD, config.MAX_PERIOD)]
    if len(invalid_periods) > 0:
        errors.append(f"Found {len(invalid_periods)} rows with invalid PERIOD (must be {config.MIN_PERIOD}-{config.MAX_PERIOD})")
        sample_periods = df.loc[invalid_periods.index[:3], ['YEAR', 'PERIOD', 'STORE_LOCATION']]
        errors.append(f"Sample invalid rows:\n{sample_periods.to_string()}")
    
    # 7. Check data types for financial columns - convert to numeric
    numeric_cols = config.FINANCIAL_COLUMNS
    for col in numeric_cols:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                errors.append(f"Error converting column {col} to numeric: {e}")
    
    # 8. Check for duplicate keys
    duplicates = df[df.duplicated(subset=config.KEY_COLUMNS, keep=False)]
    if len(duplicates) > 0:
        errors.append(f"Found {len(duplicates)} duplicate rows based on YEAR, PERIOD, STORE_LOCATION")
        sample_dups = duplicates[['YEAR', 'PERIOD', 'STORE_LOCATION']].head(5)
        errors.append(f"Sample duplicates:\n{sample_dups.to_string()}")
    
    # 9. Basic row count check
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
    print(f"Year Range: {df['YEAR'].min():.0f} - {df['YEAR'].max():.0f}")
    print(f"Period Range: {df['PERIOD'].min():.0f} - {df['PERIOD'].max():.0f}")
    print(f"\nSample of data:")
    print(df[['YEAR', 'PERIOD', 'STORE_LOCATION', 'SALES', 'COGS']].head())
    print("="*60 + "\n")