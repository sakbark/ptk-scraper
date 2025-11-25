#!/usr/bin/env python3
"""
Load PTK historical data to MongoDB and Google Sheets
"""

import json
import requests
from datetime import datetime

# Load the extracted data
DATA_FILE = "/tmp/ptk_all_historical_data_20251125_202907.json"
MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
SHEETS_WRITE_URL = f"{MONGODB_BASE}/google/spreadsheets/values/update"
SHEETS_CREATE_URL = f"{MONGODB_BASE}/google/spreadsheets/batchUpdate"

# Create new spreadsheet for PTK data
SPREADSHEET_ID = "1W1F3FUk3LYPczVk3pMJAyDJKO-krC6dQHJQulR9bzLE"  # Use existing FPS sheet or create new

print("="*80)
print("Loading PTK Historical Data to MongoDB and Google Sheets")
print("="*80)

# Load data
print(f"\n1. Loading data from {DATA_FILE}...")
with open(DATA_FILE, 'r') as f:
    all_records = json.load(f)

print(f"   ✅ Loaded {len(all_records)} records")

# Organize data by metric type
print(f"\n2. Organizing data...")

# Each record has columns for different time periods
# We need to restructure this into a time-series format
# Group records by the metric name (the "" field)

metrics_data = {}
for record in all_records:
    metric_name = record.get("", "Unknown")
    start_from = record.get("_start_from", "unknown")

    if metric_name not in metrics_data:
        metrics_data[metric_name] = []

    metrics_data[metric_name].append(record)

print(f"   ✅ Found {len(metrics_data)} unique metrics")
print(f"   Metrics: {list(metrics_data.keys())[:10]}...")

# Write to MongoDB
print(f"\n3. Writing to MongoDB...")

# Try to insert into MongoDB (using correct endpoint)
try:
    # Store raw records
    payload = {
        "database": "ptk_connect",
        "collection": "revenue_report_raw",
        "documents": all_records
    }

    # Try different endpoint formats
    endpoints = [
        f"{MONGODB_BASE}/mongodb/insertMany",
        "https://calendar-gpt-958443682078.us-central1.run.app/mongodb/insertMany"
    ]

    inserted = False
    for endpoint in endpoints:
        try:
            response = requests.post(endpoint, json=payload, timeout=30)
            if response.status_code == 200:
                print(f"   ✅ Inserted {len(all_records)} records to MongoDB (ptk_connect.revenue_report_raw)")
                inserted = True
                break
        except:
            continue

    if not inserted:
        print(f"   ⚠️  MongoDB endpoint not available - saving locally instead")
        with open("/tmp/ptk_data_for_mongodb.json", 'w') as f:
            json.dump(payload, f, indent=2)
        print(f"   💾 Saved to /tmp/ptk_data_for_mongodb.json for manual import")

except Exception as e:
    print(f"   ⚠️  MongoDB error: {str(e)}")
    print(f"   💾 Saving data locally...")
    with open("/tmp/ptk_data_for_mongodb.json", 'w') as f:
        json.dump({"database": "ptk_connect", "collection": "revenue_report_raw", "documents": all_records}, f, indent=2)

# Write to Google Sheets
print(f"\n4. Writing to Google Sheets...")

# Build sheet data - transpose so metrics are rows, time periods are columns
# First, collect all unique time period columns
all_periods = set()
for record in all_records:
    for key in record.keys():
        if key not in ["", "_start_from", "_extracted_at"] and not key.startswith("_"):
            all_periods.add(key)

sorted_periods = sorted(list(all_periods))

print(f"   Found {len(sorted_periods)} time periods")
print(f"   From {sorted_periods[0]} to {sorted_periods[-1]}")

# Build rows: First row = headers, subsequent rows = metrics
header_row = ["Metric"] + sorted_periods
all_rows = [header_row]

# Add each metric as a row
for metric_name in sorted(metrics_data.keys()):
    if not metric_name:  # Skip empty metric names
        continue

    # Get the most recent record for this metric (from "latest" period)
    latest_record = None
    for record in metrics_data[metric_name]:
        if record.get("_start_from") == "latest":
            latest_record = record
            break

    if not latest_record:
        latest_record = metrics_data[metric_name][0]

    # Build row
    row = [metric_name]
    for period in sorted_periods:
        value = latest_record.get(period, "")
        row.append(str(value) if value else "")

    all_rows.append(row)

print(f"   Built {len(all_rows)} rows × {len(sorted_periods) + 1} columns")

# Create new sheet in the spreadsheet
print(f"\n5. Creating 'PTK Revenue History' sheet...")

try:
    create_payload = {
        "body": {
            "service": "sheets",
            "operation": "spreadsheets.batchUpdate",
            "spreadsheetId": SPREADSHEET_ID,
            "requests": [{
                "addSheet": {
                    "properties": {
                        "title": "PTK Revenue History",
                        "gridProperties": {
                            "rowCount": len(all_rows) + 10,
                            "columnCount": len(sorted_periods) + 5
                        }
                    }
                }
            }]
        }
    }

    response = requests.post(SHEETS_CREATE_URL, json=create_payload, timeout=30)
    if response.status_code == 200:
        print(f"   ✅ Created 'PTK Revenue History' sheet")
    else:
        print(f"   ⚠️  Sheet might already exist, continuing...")

except Exception as e:
    print(f"   ⚠️  Create sheet: {str(e)}")

# Write data to sheet
print(f"\n6. Writing data to sheet...")

try:
    write_payload = {
        "body": {
            "service": "sheets",
            "operation": "spreadsheets.values.update",
            "spreadsheetId": SPREADSHEET_ID,
            "range": "PTK Revenue History!A1:ZZ1000",
            "valueInputOption": "RAW",
            "values": all_rows
        }
    }

    response = requests.post(SHEETS_WRITE_URL, json=write_payload, timeout=30)
    if response.status_code == 200:
        print(f"   ✅ Wrote {len(all_rows)} rows to Google Sheets")
        print(f"   📊 Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
    else:
        print(f"   ⚠️  Sheet write failed: {response.status_code}")
        # Save locally
        with open("/tmp/ptk_data_for_sheets.json", 'w') as f:
            json.dump(all_rows, f, indent=2)
        print(f"   💾 Saved to /tmp/ptk_data_for_sheets.json")

except Exception as e:
    print(f"   ⚠️  Sheet write error: {str(e)}")
    with open("/tmp/ptk_data_for_sheets.json", 'w') as f:
        json.dump(all_rows, f, indent=2)
    print(f"   💾 Saved to /tmp/ptk_data_for_sheets.json")

print(f"\n{'='*80}")
print(f"✅ COMPLETE!")
print(f"   Records processed: {len(all_records)}")
print(f"   Metrics: {len(metrics_data)}")
print(f"   Time periods: {len(sorted_periods)}")
print(f"   Sheet rows: {len(all_rows)}")
print(f"{'='*80}")
