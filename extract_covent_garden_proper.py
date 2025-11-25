#!/usr/bin/env python3
"""
Extract Covent Garden FPS data from statistics tabs (same structure as Islington)
"""

import os
import requests
from pymongo import MongoClient
from datetime import datetime

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://evolution_admin:th5ozXlvZzgoeBRb@cluster0.irfuesb.mongodb.net/?retryWrites=true&w=majority")
MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
SPREADSHEET_ID = "1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ"
EXECUTION_ID = open("/tmp/cg_execution_id.txt").read().strip()

print("="*80)
print("EXTRACTING COVENT GARDEN FPS - PROPER STATISTICS TABS")
print("="*80)

# Statistics tabs to extract
stats_tabs = ["2023 Statistics", "2024 Statistics", "2025 Statistics"]
all_data = {}

for tab_name in stats_tabs:
    print(f"\n{'='*80}")
    print(f"Processing: {tab_name}")
    print(f"{'='*80}")

    # Extract data from this tab
    payload = {
        "body": {
            "service": "sheets",
            "operation": "spreadsheets.values.get",
            "spreadsheetId": SPREADSHEET_ID,
            "range": f"'{tab_name}'!A1:ZZ1000"
        },
        "account": "work"
    }

    response = requests.post(
        f"{MONGODB_BASE}/proxy",
        json=payload,
        timeout=30
    )

    if response.status_code != 200:
        print(f"   ❌ Failed to read {tab_name}: {response.status_code}")
        continue

    result = response.json()
    values = result.get('values', [])

    print(f"   ✅ Read {len(values)} rows")

    # Show first few rows to understand structure
    print(f"\n   First 5 rows:")
    for i, row in enumerate(values[:5]):
        print(f"      Row {i+1}: {row[:5] if len(row) > 5 else row}...")

    # Parse the data (row 2 has headers, data from row 3+)
    if len(values) > 2:
        headers = values[1]  # Row 2 has the real headers
        print(f"\n   Headers: {headers[:10] if len(headers) > 10 else headers}...")

        records = []
        for i in range(2, len(values)):  # Start from row 3
            row = values[i]
            if not row or len(row) < 2:  # Skip empty rows
                continue

            # Period should be in column B (index 1)
            period = row[1] if len(row) > 1 else ""
            if not period or period == "":  # Skip if no period
                continue

            # Build record
            record = {"Period": period}
            for j, header in enumerate(headers):
                if header and j < len(row) and header != "Period":
                    record[header] = row[j]

            # Add metadata
            record['source_spreadsheet'] = SPREADSHEET_ID
            record['source_tab'] = tab_name
            record['extraction_date'] = datetime.utcnow().isoformat()
            record['data_type'] = 'covent_garden_statistics'

            records.append(record)

        all_data[tab_name] = records
        print(f"   ✅ Extracted {len(records)} period records")

# Combine all records
all_records = []
for tab_name, records in all_data.items():
    all_records.extend(records)

print(f"\n{'='*80}")
print(f"TOTAL RECORDS EXTRACTED: {len(all_records)}")
print(f"{'='*80}")

# Write to MongoDB
print(f"\nWriting to MongoDB...")

client = MongoClient(MONGODB_URI)
db = client["franchise_fps"]

# Clear existing Covent Garden data
collection = db["covent_garden_statistics_by_period"]
delete_result = collection.delete_many({})
print(f"   Deleted {delete_result.deleted_count} existing records")

# Insert new data
if all_records:
    insert_result = collection.insert_many(all_records)
    print(f"   ✅ Inserted {len(insert_result.inserted_ids)} records")
else:
    print(f"   ⚠️  No records to insert")

client.close()

print(f"\n{'='*80}")
print("EXTRACTION COMPLETE")
print(f"{'='*80}")
print(f"Collection: franchise_fps.covent_garden_statistics_by_period")
print(f"Records: {len(all_records)}")
