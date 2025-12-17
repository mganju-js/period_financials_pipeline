from utils.snowflake_utils import get_snowflake_connection
import config

def test_connection():
    """Test Snowflake connection and show basic info."""
    try:
        print("Testing Snowflake connection...")
        print(f"Account: {config.SNOWFLAKE_CONFIG['account']}")
        print(f"User: {config.SNOWFLAKE_CONFIG['user']}")
        print(f"Database: {config.SNOWFLAKE_CONFIG['database']}")
        print(f"Schema: {config.SNOWFLAKE_CONFIG['schema']}")
        print("-" * 50)
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        
        print("✓ Connection successful!")
        print(f"Connected as: {result[0]}")
        print(f"Role: {result[1]}")
        print(f"Database: {result[2]}")
        print(f"Schema: {result[3]}")
        
        # Check if target table exists
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {config.SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{config.SNOWFLAKE_SCHEMA}' 
            AND TABLE_NAME = '{config.TARGET_TABLE}'
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            print(f"✓ Target table {config.TARGET_TABLE} exists")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {config.TARGET_TABLE}")
            row_count = cursor.fetchone()[0]
            print(f"  Current rows in table: {row_count}")
        else:
            print(f"✗ Target table {config.TARGET_TABLE} does not exist!")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()