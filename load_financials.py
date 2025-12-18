import argparse
import logging
import sys
from datetime import datetime
import pandas as pd
import os

from utils.file_utils import read_excel_file, get_filename_from_path
from utils.validation import validate_dataframe, print_validation_summary
from utils.snowflake_utils import (
    get_snowflake_connection,
    create_stage_if_not_exists,
    upload_to_stage,
    create_temp_table,
    load_stage_to_temp,
    merge_temp_to_target
)

# Setup logging
os.makedirs('logs', exist_ok=True)
log_filename = f"logs/load_financials_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main(excel_file: str):
    """Main pipeline execution."""
    start_time = datetime.now()
    logger.info("="*60)
    logger.info("STORE FINANCIALS PIPELINE STARTED")
    logger.info(f"Input file: {excel_file}")
    logger.info("="*60)
    
    conn = None
    temp_csv = None
    
    try:
        # Step 1: Read Excel file
        logger.info("Step 1: Reading Excel file...")
        df = read_excel_file(excel_file)
        logger.info(f"Loaded {len(df)} rows from Excel")
        
        # Step 2: Validate data
        logger.info("Step 2: Validating data...")
        is_valid, errors = validate_dataframe(df, excel_file)
        
        if not is_valid:
            logger.error("VALIDATION FAILED!")
            for error in errors:
                logger.error(f"  - {error}")
            print("\n❌ VALIDATION FAILED - Pipeline stopped")
            print("Errors found:")
            for error in errors:
                print(f"  - {error}")
            return False
        
        logger.info("✓ Validation passed")
        print_validation_summary(df)
        
        # Step 3: Connect to Snowflake
        logger.info("Step 3: Connecting to Snowflake...")
        conn = get_snowflake_connection()
        
        # Step 4: Create stage
        logger.info("Step 4: Setting up Snowflake stage...")
        create_stage_if_not_exists(conn)
        
        # Step 5: Save to CSV and upload to stage
        logger.info("Step 5: Uploading data to stage...")
        temp_csv = f"temp_financials_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(temp_csv, index=False)
        
        stage_file = f"financials_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        upload_to_stage(conn, temp_csv, stage_file)
        
        # Step 6: Create temp table
        logger.info("Step 6: Creating temporary table...")
        create_temp_table(conn)
        
        # Step 7: Load to temp table
        logger.info("Step 7: Loading data to temporary table...")
        load_stage_to_temp(conn, stage_file)
        
        # Step 8: Merge to target table
        logger.info("Step 8: Merging data to target table...")
        source_filename = get_filename_from_path(excel_file)
        rows_inserted, rows_updated = merge_temp_to_target(conn, source_filename)
        
        # Success!
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Rows inserted: {rows_inserted}")
        logger.info(f"Rows updated: {rows_updated}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("="*60)
        
        print("\n✓ SUCCESS!")
        print(f"  Rows inserted: {rows_inserted}")
        print(f"  Rows updated: {rows_updated}")
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  Log file: {log_filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        print(f"\n❌ ERROR: {e}")
        print(f"Check log file for details: {log_filename}")
        return False
        
    finally:
        # Cleanup
        if temp_csv and os.path.exists(temp_csv):
            os.remove(temp_csv)
            logger.info(f"Cleaned up temporary file: {temp_csv}")
        
        if conn:
            conn.close()
            logger.info("Closed Snowflake connection")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load store financials to Snowflake')
    parser.add_argument('--file', required=True, help='Path to Excel (.xlsx, .xls) or CSV file')
    
    args = parser.parse_args()
    
    success = main(args.file)
    sys.exit(0 if success else 1)