#!/usr/bin/env python3
"""
Debug Covent Garden FPS Mirror Sheet - Why is it empty?
"""

import os
import requests
from pymongo import MongoClient
from datetime import datetime

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://evolution_admin:th5ozXlvZzgoeBRb@cluster0.irfuesb.mongodb.net/?retryWrites=true&w=majority")
MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
MIRROR_SHEET_ID = "1g5wrND10Ut-GsVPi8D_3bSWl8OuQCww7W5mVOtl4i4U"

print("="*80)
print("DEBUGGING COVENT GARDEN FPS MIRROR SHEET")
print("="*80)

# STEP 1: Read data from MongoDB
print("\n1. Reading data from MongoDB...")
client = MongoClient(MONGODB_URI)
db = client["franchise_fps"]
collection = db["covent_garden_fps_by_period"]

all_records = list(collection.find({}))
print(f"   ✅ Found {len(all_records)} records in MongoDB")

if len(all_records) == 0:
    print("   ❌ ERROR: No data in MongoDB!")
    exit(1)

# Show sample record
print(f"\n   Sample record:")
sample = all_records[0]
for key, value in sample.items():
    if key != "_id":
        print(f"      {key}: {value}")

# STEP 2: Organize data for transposed format
print("\n2. Organizing data for transposed sheet format...")

# Group by period
periods_dict = {}
all_metrics = set()

for record in all_records:
    period = record.get("Period", "Unknown")
    periods_dict[period] = record

    # Collect all metric names (exclude metadata fields)
    for key in record.keys():
        if key not in ["_id", "Period", "_metadata", "source_spreadsheet", "extraction_date", "data_type"]:
            all_metrics.add(key)

all_metrics = sorted(list(all_metrics))
periods = sorted(periods_dict.keys())

print(f"   Periods: {len(periods)}")
print(f"   Metrics: {len(all_metrics)}")
print(f"   Expected cells: {len(periods) * len(all_metrics)}")

# STEP 3: Build transposed array (periods as columns, metrics as rows)
print("\n3. Building transposed array...")

rows = []

# Header row: ["Metric"] + list of periods
header = ["Metric"] + periods
rows.append(header)
print(f"   Header: {len(header)} columns")

# Each metric becomes a row
for metric in all_metrics:
    row = [metric]
    for period in periods:
        record = periods_dict.get(period, {})
        value = record.get(metric, "")
        row.append(value)
    rows.append(row)

print(f"   ✅ Built {len(rows)} rows × {len(rows[0])} columns")
print(f"   Total cells to write: {len(rows) * len(rows[0])}")

# Show first few rows
print(f"\n   First 3 rows preview:")
for i in range(min(3, len(rows))):
    print(f"      Row {i+1}: {rows[i][:3]}... (showing first 3 cells)")

# STEP 4: Write to Google Sheets with detailed logging
print("\n4. Writing to Google Sheets...")
print(f"   Sheet ID: {MIRROR_SHEET_ID}")
print(f"   Range: Sheet1!A1")

write_payload = {
    "range": "Sheet1!A1",
    "values": rows,
    "valueInputOption": "RAW",
    "account": "work"
}

print(f"\n   Payload size:")
print(f"      Rows: {len(write_payload['values'])}")
print(f"      Columns: {len(write_payload['values'][0]) if write_payload['values'] else 0}")

try:
    write_response = requests.post(
        f"{MONGODB_BASE}/sheets/{MIRROR_SHEET_ID}/values/update",
        json=write_payload,
        timeout=120
    )

    print(f"\n   Response Status: {write_response.status_code}")

    if write_response.status_code == 200:
        result = write_response.json()
        print(f"   ✅ Write successful!")
        print(f"\n   Response details:")
        print(f"      spreadsheetId: {result.get('spreadsheetId', 'N/A')}")
        print(f"      updatedRange: {result.get('updatedRange', 'N/A')}")
        print(f"      updatedRows: {result.get('updatedRows', 'N/A')}")
        print(f"      updatedColumns: {result.get('updatedColumns', 'N/A')}")
        print(f"      updatedCells: {result.get('updatedCells', 'N/A')}")
    else:
        print(f"   ❌ Write failed!")
        print(f"   Status: {write_response.status_code}")
        print(f"   Response: {write_response.text[:500]}")

except Exception as e:
    print(f"   ❌ Exception during write: {e}")

# STEP 5: Verify by reading back
print("\n5. Verifying by reading back from sheet...")

try:
    read_payload = {
        "body": {
            "service": "sheets",
            "operation": "spreadsheets.values.get",
            "spreadsheetId": MIRROR_SHEET_ID,
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
        print(f"   ✅ Read back {len(values)} rows")
        print(f"\n   First 3 rows from sheet:")
        for i, row in enumerate(values[:3]):
            print(f"      Row {i+1}: {row}")
    else:
        print(f"   ❌ Read failed: {read_response.status_code}")
        print(f"   Response: {read_response.text[:200]}")

except Exception as e:
    print(f"   ❌ Exception during read: {e}")

print("\n" + "="*80)
print("DEBUG COMPLETE")
print("="*80)
print(f"\nSheet URL: https://docs.google.com/spreadsheets/d/{MIRROR_SHEET_ID}/edit")

client.close()
