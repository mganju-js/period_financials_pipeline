import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake Configuration - SANDBOX
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': 'DB_SANDBOX',  # Changed to sandbox
    'schema': 'UPLOADS',  # Changed to uploads
    'role': os.getenv('SNOWFLAKE_ROLE'),
    'authenticator': os.getenv('SNOWFLAKE_AUTHENTICATOR', 'externalbrowser')
}

# Remove password from config if using SSO
if SNOWFLAKE_CONFIG['authenticator'] == 'externalbrowser':
    SNOWFLAKE_CONFIG.pop('password', None)
else:
    SNOWFLAKE_CONFIG['password'] = os.getenv('SNOWFLAKE_PASSWORD')

# These need to be module-level variables too
SNOWFLAKE_DATABASE = 'DB_SANDBOX'
SNOWFLAKE_SCHEMA = 'UPLOADS'

# Table Configuration - TEST TABLE
TARGET_TABLE = 'RAW_STORE_FINANCIALS_TEST'
TEMP_TABLE = 'RAW_STORE_FINANCIALS_TEST_TEMP'
STAGE_NAME = 'FINANCIALS_STAGE_TEST'

# Required Columns - in exact order from your table
REQUIRED_COLUMNS = [
    'YEAR', 'PERIOD', 'OPENED', 'STORE_LOCATION', 'SALES',
    'BREAD_COGS', 'PROTEIN_COGS', 'PRODUCE_COGS', 'DAIRY_COGS',
    'GROCERY_COGS', 'BEVERAGE_COGS', 'SNACK_COGS', 'PAPER_SUPPLIES',
    'COGS', 'ARTL_RTL', 'HOURLY_CREW', 'TOTAL_LABOR_COST',
    'TOTAL_PR_TAXES_AND_BENEFITS', 'RENT', 'GAS_AND_ELECTRIC',
    'WATER', 'CAM_CHARGES', 'COMMERCIAL_RENT_TAX', 'REAL_ESTATE_TAX',
    'GENERAL_LIABILITY_INSURANCE', 'SUPPLIES', 'DISCRETIONARY',
    'HVAC_REPAIRS', 'BUILDING_REPAIRS', 'PLUMBING_REPAIRS',
    'ELECTRIC_REPAIRS', 'REFRIGERATION_REPAIRS', 'COMPUTER_REPAIRS',
    'CLEANING_GREASE_TRAP', 'WASTE_REMOVAL', 'PEST_CONTROL',
    'ALARM_SYSTEM', 'TELEPHONE_AND_INTERNET', 'WEB_EXPENSES',
    'PROMOTIONS', 'DELIVERY_FEES', 'CATERING_FEES', 'PROCESSING_FEES',
    'INTERNAL_GA', 'TOTAL_GA'
]

# Key columns for merge
KEY_COLUMNS = ['YEAR', 'PERIOD', 'STORE_LOCATION']

# Financial columns (everything except keys and OPENED)
FINANCIAL_COLUMNS = [col for col in REQUIRED_COLUMNS 
                     if col not in KEY_COLUMNS and col != 'OPENED']

# Validation Rules
MIN_YEAR = 2019
MAX_YEAR = 2030
MIN_PERIOD = 1
MAX_PERIOD = 13