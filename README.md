# Store Financials Pipeline

Python pipeline to load monthly store financial data into Snowflake Bronze layer with automatic upsert logic.

## Features

- ✅ Reads Excel files with store financial data
- ✅ Validates data quality (column presence, data types, ranges)
- ✅ Handles period format conversion (P1 → 1)
- ✅ Upsert logic: Updates changed records, inserts new records, leaves unchanged records untouched
- ✅ Automatic audit timestamps (created_at, updated_at)
- ✅ Comprehensive logging
- ✅ Sandbox testing mode

## Setup

1. **Install dependencies:**
```bash
conda create -n financials_pipeline python=3.11
conda activate financials_pipeline
pip install -r requirements.txt
```

2. **Configure Snowflake credentials:**
   - Copy `.env.example` to `.env`
   - Fill in your Snowflake credentials

3. **Verify connection:**
```bash
python test_connection.py
```

## Usage

### Production Load (DB_BRONZE)
```bash
python load_financials.py --file "path/to/Store_Financials.xlsx"
```

### Sandbox Testing (DB_SANDBOX)
```bash
python load_financials_sandbox.py --file "path/to/Store_Financials.xlsx"
```

## Excel File Requirements

Your Excel file must contain these columns:
- **Key columns:** YEAR, PERIOD, STORE_LOCATION
- **Metadata:** OPENED
- **Financial columns:** SALES, BREAD_COGS, PROTEIN_COGS, etc. (41 financial metrics)

**Note:** PERIOD can be formatted as "P1", "P01", or "1" - the pipeline converts automatically.

## How It Works

1. **Extract:** Reads Excel file and converts to DataFrame
2. **Validate:** Checks column presence, data types, value ranges, duplicates
3. **Stage:** Uploads data to Snowflake internal stage
4. **Load:** Copies to temporary table
5. **Merge:** 
   - **New records** → INSERT with created_at timestamp, updated_at = NULL
   - **Changed records** → UPDATE with updated_at timestamp
   - **Unchanged records** → No action (keeps original timestamps)

## Validation Rules

- Required columns must exist
- No nulls in key columns (YEAR, PERIOD, STORE_LOCATION)
- YEAR must be 2020-2030
- PERIOD must be 1-13
- Financial columns must be numeric
- No duplicate keys

## Error Handling

If validation fails, the pipeline stops immediately and no data is loaded. Check the log file in `logs/` for details.

## Audit Columns

All records include:
- `created_at`: Timestamp when record was first inserted
- `updated_at`: Timestamp when record was last modified (NULL if never updated)

## Project Structure
```
period_financials_pipeline/
├── config.py                 # Production configuration
├── config_sandbox.py         # Sandbox configuration
├── load_financials.py        # Production pipeline
├── load_financials_sandbox.py # Sandbox pipeline
├── test_connection.py        # Connection test utility
├── utils/
│   ├── snowflake_utils.py   # Snowflake operations
│   ├── validation.py         # Data validation
│   └── file_utils.py         # Excel handling
├── logs/                     # Execution logs
└── README.md
```

## Troubleshooting

**Issue:** Authentication fails
- **Solution:** Make sure you're using SSO (externalbrowser) authentication in `.env`

**Issue:** Validation fails
- **Solution:** Check the log file for specific validation errors

**Issue:** All records showing as updated
- **Solution:** Make sure you're using the latest version with change detection logic

## Best Practices

1. **Always test in sandbox first** before loading to production
2. **Review validation summary** before confirming loads
3. **Keep Excel files organized** with clear naming conventions (e.g., `Store_Financials_2024P13.xlsx`)
4. **Check logs** after each run for any warnings or errors