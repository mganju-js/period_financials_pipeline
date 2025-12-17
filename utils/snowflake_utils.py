import snowflake.connector
from snowflake.connector import DictCursor
import config
import pandas as pd
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create and return Snowflake connection."""
    try:
        conn = snowflake.connector.connect(
            **config.SNOWFLAKE_CONFIG
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def create_stage_if_not_exists(conn):
    """Create internal stage for file uploads if it doesn't exist."""
    cursor = conn.cursor()
    try:
        create_stage_sql = f"""
        CREATE STAGE IF NOT EXISTS {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.{config.STAGE_NAME}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
        """
        cursor.execute(create_stage_sql)
        logger.info(f"Stage {config.STAGE_NAME} is ready")
    except Exception as e:
        logger.error(f"Error creating stage: {e}")
        raise
    finally:
        cursor.close()


def upload_to_stage(conn, local_file: str, stage_file: str):
    """Upload file to Snowflake stage."""
    cursor = conn.cursor()
    try:
        put_sql = f"PUT file://{local_file} @{config.STAGE_NAME}/{stage_file} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        logger.info(f"Uploaded {local_file} to stage")
    except Exception as e:
        logger.error(f"Error uploading to stage: {e}")
        raise
    finally:
        cursor.close()


def create_temp_table(conn):
    """Create temporary table for staging data."""
    cursor = conn.cursor()
    try:
        # Drop if exists
        cursor.execute(f"DROP TABLE IF EXISTS {config.TEMP_TABLE}")
        
        # Create temp table with same structure as target
        create_sql = f"""
        CREATE TEMPORARY TABLE {config.TEMP_TABLE} LIKE {config.TARGET_TABLE}
        """
        cursor.execute(create_sql)
        logger.info(f"Created temporary table {config.TEMP_TABLE}")
    except Exception as e:
        logger.error(f"Error creating temp table: {e}")
        raise
    finally:
        cursor.close()


def load_stage_to_temp(conn, stage_file: str):
    """Load data from stage to temporary table."""
    cursor = conn.cursor()
    try:
        # Build the column list (exclude audit columns from COPY)
        data_columns = ", ".join(config.REQUIRED_COLUMNS)
        
        copy_sql = f"""
        COPY INTO {config.TEMP_TABLE} ({data_columns})
        FROM @{config.STAGE_NAME}/{stage_file}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'ABORT_STATEMENT'
        """
        cursor.execute(copy_sql)
        result = cursor.fetchone()
        logger.info(f"Loaded data to temp table: {result}")
        return result
    except Exception as e:
        logger.error(f"Error loading to temp table: {e}")
        raise
    finally:
        cursor.close()


def merge_temp_to_target(conn, source_filename: str):
    """Merge data from temp table to target table."""
    cursor = conn.cursor()
    
    # Build UPDATE SET clause for all financial columns
    update_set_clause = ",\n        ".join([
        f"target.{col} = source.{col}" for col in config.FINANCIAL_COLUMNS
    ])
    
    # Build condition to check if ANY value has changed
    # This prevents updating unchanged rows
    value_changed_conditions = " OR ".join([
        f"target.{col} != source.{col} OR (target.{col} IS NULL AND source.{col} IS NOT NULL) OR (target.{col} IS NOT NULL AND source.{col} IS NULL)"
        for col in config.FINANCIAL_COLUMNS
    ])
    
    # Also check if OPENED changed
    value_changed_conditions += " OR target.OPENED != source.OPENED OR (target.OPENED IS NULL AND source.OPENED IS NOT NULL) OR (target.OPENED IS NOT NULL AND source.OPENED IS NULL)"
    
    # Build INSERT columns and values (including audit columns)
    all_cols = config.REQUIRED_COLUMNS
    insert_cols = ", ".join(all_cols + ['created_at', 'updated_at'])
    insert_vals = ", ".join([f"source.{col}" for col in all_cols])
    
    merge_sql = f"""
    MERGE INTO {config.TARGET_TABLE} target
    USING (
        SELECT *,
               CURRENT_TIMESTAMP() as load_timestamp
        FROM {config.TEMP_TABLE}
    ) source
    ON target.YEAR = source.YEAR 
       AND target.PERIOD = source.PERIOD 
       AND target.STORE_LOCATION = source.STORE_LOCATION
    WHEN MATCHED AND ({value_changed_conditions}) THEN 
      UPDATE SET 
        {update_set_clause},
        target.OPENED = source.OPENED,
        target.updated_at = source.load_timestamp
    WHEN NOT MATCHED THEN 
      INSERT ({insert_cols})
      VALUES ({insert_vals}, source.load_timestamp, NULL)
    """
    
    try:
        cursor.execute(merge_sql)
        result = cursor.fetchone()
        rows_inserted = result[0] if result else 0
        rows_updated = result[1] if result else 0
        
        logger.info(f"Merge complete - Inserted: {rows_inserted}, Updated: {rows_updated}")
        return rows_inserted, rows_updated
    except Exception as e:
        logger.error(f"Error during merge: {e}")
        raise
    finally:
        cursor.close()