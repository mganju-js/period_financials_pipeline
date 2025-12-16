# Store Financials Pipeline

Python pipeline to load monthly store financial data into Snowflake Bronze layer.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create `.env` file with Snowflake credentials (see `.env.example`)

3. Ensure your Excel file has the required columns

## Usage
```bash
python load_financials.py --file "path/to/Store_Financials_Dec2024.xlsx"
```

## Pipeline Steps

1. Read Excel file
2. Validate data (columns, types, ranges)
3. Upload to Snowflake stage
4. Load to temporary table
5. Merge (upsert) to target table
6. Log results

## Validation Rules

- Required columns must exist
- No nulls in key columns (YEAR, PERIOD, STORE_LOCATION)
- YEAR must be 2020-2030
- PERIOD must be 1-13
- Financial columns must be numeric
- No duplicate keys

## Error Handling

If validation fails, the pipeline stops and no data is loaded.
Check the log file in `logs/` for details.