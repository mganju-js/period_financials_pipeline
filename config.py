import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE')
}

# Table Configuration
TARGET_TABLE = 'RAW_STORE_FINANCIALS'
TEMP_TABLE = 'RAW_STORE_FINANCIALS_TEMP'
STAGE_NAME = 'FINANCIALS_STAGE'

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