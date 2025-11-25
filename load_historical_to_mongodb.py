#!/usr/bin/env python3
"""
Load PTK historical data to MongoDB and create comprehensive mirror sheet
"""

import os
import json
import requests
from pymongo import MongoClient
from datetime import datetime
from glob import glob

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://evolution_admin:th5ozXlvZzgoeBRb@cluster0.irfuesb.mongodb.net/?retryWrites=true&w=majority")
MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"

print("="*80)
print("LOADING PTK HISTORICAL DATA TO MONGODB")
print("="*80)

# Find the latest historical data file
json_files = glob("/tmp/ptk_all_historical_data_*.json")
if not json_files:
    print("❌ No historical data file found!")
    exit(1)

latest_file = sorted(json_files)[-1]
print(f"\n1. Loading data from: {latest_file}")

with open(latest_file, 'r') as f:
    all_records = json.load(f)

print(f"   ✅ Loaded {len(all_records)} records")

# Show sample
print(f"\n   Sample record:")
sample = all_records[0]
for key in list(sample.keys())[:10]:
    print(f"      {key}: {sample[key]}")

# Write to MongoDB
print(f"\n2. Writing to MongoDB...")

client = MongoClient(MONGODB_URI)
db = client["ptk_connect"]
collection = db["revenue_report_historical"]

# Clear existing historical data
delete_result = collection.delete_many({})
print(f"   Deleted {delete_result.deleted_count} existing records")

# Insert new historical data
insert_result = collection.insert_many(all_records)
print(f"   ✅ Inserted {len(insert_result.inserted_ids)} records")

# Read back and organize for Sheets
print(f"\n3. Organizing data for Google Sheets...")

records = list(collection.find({}))

# Get all unique time periods (column headers)
all_periods = set()
metric_names = set()

for record in records:
    for key in record.keys():
        if key not in ["_id", "_start_from", "_extracted_at", ""]:
            all_periods.add(key)
        if key == "":
            metric_names.add(record[key])

# Sort periods chronologically
periods = sorted(list(all_periods))
print(f"   Time periods: {len(periods)}")
print(f"   First few: {periods[:5]}")
print(f"   Last few: {periods[-5:]}")

# Create metrics dictionary
metrics_dict = {}
for record in records:
    metric_name = record.get("", "Unknown")
    if metric_name not in metrics_dict:
        metrics_dict[metric_name] = {}

    for period in periods:
        if period in record:
            metrics_dict[metric_name][period] = record[period]

print(f"   Metrics: {len(metrics_dict)}")

# Build transposed array (metrics as rows, periods as columns)
print(f"\n4. Building transposed array...")

rows = []

# Header row
header = ["Metric"] + periods
rows.append(header)

# Each metric becomes a row
for metric_name in sorted(metrics_dict.keys()):
    row = [metric_name]
    for period in periods:
        value = metrics_dict[metric_name].get(period, "")
        row.append(value)
    rows.append(row)

print(f"   ✅ Built {len(rows)} rows × {len(rows[0])} columns")
print(f"   Total cells: {len(rows) * len(rows[0])}")

# Create Google Sheet
print(f"\n5. Creating Google Sheet...")

create_payload = {
    "body": {
        "title": "PTK Revenue Report - Historical (2016-2025)"
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
    exit(1)

sheet_id = create_response.json()['spreadsheetId']
print(f"   ✅ Created sheet: {sheet_id}")

# Write data
print(f"\n6. Writing {len(rows) * len(rows[0])} cells to sheet...")

write_payload = {
    "range": "Sheet1!A1",
    "values": rows,
    "valueInputOption": "RAW",
    "account": "work"
}

write_response = requests.post(
    f"{MONGODB_BASE}/sheets/{sheet_id}/values/update",
    json=write_payload,
    timeout=180
)

if write_response.status_code == 200:
    result = write_response.json()
    print(f"   ✅ Write successful!")
    print(f"      Updated cells: {result.get('updatedCells')}")
    print(f"      Updated rows: {result.get('updatedRows')}")
    print(f"      Updated columns: {result.get('updatedColumns')}")
else:
    print(f"   ❌ Write failed: {write_response.status_code}")
    print(f"   Response: {write_response.text[:500]}")

# Document to SSM
print(f"\n7. Writing to SSM...")

execution_report = {
    "workflow_id": "ptk_historical_import_nov25_2025",
    "execution_id": f"exec_ptk_historical_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
    "status": "completed",
    "completed_at": datetime.utcnow().isoformat(),
    "summary": {
        "source_file": latest_file,
        "total_records": len(all_records),
        "time_periods": len(periods),
        "metrics_count": len(metrics_dict),
        "mongodb_collection": "ptk_connect.revenue_report_historical",
        "mirror_sheet_id": sheet_id,
        "mirror_sheet_url": f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit",
        "cells_written": len(rows) * len(rows[0])
    }
}

ssm_payload = {
    "database": "scheduler_memory",
    "collection": "workflow_executions",
    "document": execution_report
}

ssm_response = requests.post(
    f"{MONGODB_BASE}/mongodb/insert",
    json=ssm_payload,
    timeout=30
)

if ssm_response.status_code == 200:
    print(f"   ✅ SSM documentation complete")
else:
    print(f"   ⚠️  SSM failed: {ssm_response.status_code}")

print(f"\n{'='*80}")
print("✅ PTK HISTORICAL DATA IMPORT COMPLETE")
print(f"{'='*80}")
print(f"\nMongoDB: ptk_connect.revenue_report_historical")
print(f"Records: {len(all_records)}")
print(f"Time Periods: {len(periods)} ({periods[0]} → {periods[-1]})")
print(f"Metrics: {len(metrics_dict)}")
print(f"\nMirror Sheet: https://docs.google.com/spreadsheets/d/{sheet_id}/edit")
print(f"Cells: {len(rows)} rows × {len(rows[0])} columns = {len(rows) * len(rows[0])} cells")

client.close()
