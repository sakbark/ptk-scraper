#!/usr/bin/env python3
"""
Create Covent Garden FPS Mirror Sheet from MongoDB
"""

import os
import requests
from pymongo import MongoClient
from datetime import datetime

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://evolution_admin:th5ozXlvZzgoeBRb@cluster0.irfuesb.mongodb.net/?retryWrites=true&w=majority")
MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"

print("="*80)
print("CREATING COVENT GARDEN FPS MIRROR SHEET")
print("="*80)

# Read from MongoDB
print("\n1. Reading from MongoDB...")
client = MongoClient(MONGODB_URI)
db = client["franchise_fps"]
collection = db["covent_garden_statistics_by_period"]

all_records = list(collection.find({}))
print(f"   ✅ Found {len(all_records)} records")

if len(all_records) == 0:
    print("   ❌ No data in MongoDB!")
    exit(1)

# Organize for transposed format (periods as columns, metrics as rows)
print("\n2. Organizing data for transposed sheet...")

# Group by period
periods_dict = {}
all_metrics = set()

for record in all_records:
    period = record.get("Period", "Unknown")

    # Store the record by period (we might have duplicates from different year tabs)
    if period not in periods_dict:
        periods_dict[period] = {}

    # Merge metrics from this record
    for key, value in record.items():
        if key not in ["_id", "Period", "source_spreadsheet", "source_tab", "extraction_date", "data_type"]:
            periods_dict[period][key] = value
            all_metrics.add(key)

all_metrics = sorted(list(all_metrics))
periods = sorted(periods_dict.keys())

print(f"   Unique periods: {len(periods)}")
print(f"   Metrics: {len(all_metrics)}")
print(f"   Metrics: {all_metrics[:5]}...")

# Build transposed array
print("\n3. Building transposed array...")

rows = []

# Header row
header = ["Metric"] + periods
rows.append(header)

# Each metric becomes a row
for metric in all_metrics:
    row = [metric]
    for period in periods:
        value = periods_dict.get(period, {}).get(metric, "")
        row.append(value)
    rows.append(row)

print(f"   ✅ Built {len(rows)} rows × {len(rows[0])} columns")
print(f"   Total cells: {len(rows) * len(rows[0])}")

# Create new sheet
print("\n4. Creating new Google Sheet...")

create_payload = {
    "body": {
        "title": "Covent Garden FPS - Mirror (All Data)"
    },
    "account": "work"
}

create_response = requests.post(
    f"{MONGODB_BASE}/sheets/spreadsheets/create",
    json=create_payload,
    timeout=30
)

if create_response.status_code != 200:
    print(f"   ❌ Failed to create sheet: {create_response.status_code}")
    print(f"   Response: {create_response.text[:500]}")
    exit(1)

sheet_id = create_response.json()['spreadsheetId']
print(f"   ✅ Created sheet: {sheet_id}")

# Write data
print("\n5. Writing data to sheet...")

write_payload = {
    "range": "Sheet1!A1",
    "values": rows,
    "valueInputOption": "RAW",
    "account": "work"
}

write_response = requests.post(
    f"{MONGODB_BASE}/sheets/{sheet_id}/values/update",
    json=write_payload,
    timeout=120
)

if write_response.status_code == 200:
    result = write_response.json()
    print(f"   ✅ Write successful!")
    print(f"      Updated range: {result.get('updatedRange')}")
    print(f"      Updated rows: {result.get('updatedRows')}")
    print(f"      Updated columns: {result.get('updatedColumns')}")
    print(f"      Updated cells: {result.get('updatedCells')}")
else:
    print(f"   ❌ Write failed: {write_response.status_code}")
    print(f"   Response: {write_response.text[:500]}")
    exit(1)

# Verify
print("\n6. Verifying data...")

read_payload = {
    "body": {
        "service": "sheets",
        "operation": "spreadsheets.values.get",
        "spreadsheetId": sheet_id,
        "range": "Sheet1!A1:C3"
    },
    "account": "work"
}

read_response = requests.post(
    f"{MONGODB_BASE}/proxy",
    json=read_payload,
    timeout=30
)

if read_response.status_code == 200:
    read_result = read_response.json()
    values = read_result.get('values', [])
    print(f"   ✅ Verification successful - read {len(values)} rows")
    print(f"\n   First 3 rows:")
    for i, row in enumerate(values):
        print(f"      Row {i+1}: {row}")
else:
    print(f"   ⚠️  Could not verify: {read_response.status_code}")

print(f"\n{'='*80}")
print("✅ MIRROR SHEET CREATED SUCCESSFULLY")
print(f"{'='*80}")
print(f"\nSheet URL: https://docs.google.com/spreadsheets/d/{sheet_id}/edit")
print(f"Source: MongoDB franchise_fps.covent_garden_statistics_by_period")
print(f"Records: {len(all_records)}")
print(f"Periods: {len(periods)}")
print(f"Metrics: {len(all_metrics)}")

client.close()
