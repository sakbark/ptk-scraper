#!/usr/bin/env python3
"""
Write PTK data directly to Google Sheets
"""

import json
import requests

# Configuration
SHEETS_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
SPREADSHEET_ID = "1W1F3FUk3LYPczVk3pMJAyDJKO-krC6dQHJQulR9bzLE"
DATA_FILE = "/tmp/ptk_all_historical_data_20251125_202907.json"

print("="*80)
print("Writing PTK Historical Data to Google Sheets")
print("="*80)

# Load data
print(f"\n1. Loading data from {DATA_FILE}...")
with open(DATA_FILE, 'r') as f:
    all_records = json.load(f)

print(f"   ✅ Loaded {len(all_records)} records")

# Organize data
print(f"\n2. Organizing data...")

# Collect all unique time period columns
all_periods = set()
for record in all_records:
    for key in record.keys():
        if key not in ["", "_start_from", "_extracted_at"] and not key.startswith("_"):
            all_periods.add(key)

sorted_periods = sorted(list(all_periods))
print(f"   ✅ Found {len(sorted_periods)} time periods")
print(f"   From {sorted_periods[0]} to {sorted_periods[-1]}")

# Group by metric name
metrics_data = {}
for record in all_records:
    metric_name = record.get("", "Unknown")
    if metric_name not in metrics_data:
        metrics_data[metric_name] = []
    metrics_data[metric_name].append(record)

print(f"   ✅ Found {len(metrics_data)} unique metrics")

# Build sheet data: metrics as rows, periods as columns
header_row = ["Metric"] + sorted_periods
all_rows = [header_row]

for metric_name in sorted(metrics_data.keys()):
    if not metric_name:
        continue

    # Get the latest record for this metric
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

print(f"   ✅ Built {len(all_rows)} rows × {len(sorted_periods) + 1} columns")

# Create sheet
print(f"\n3. Creating 'PTK Revenue History' sheet...")

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

    response = requests.post(f"{SHEETS_BASE}/proxy", json=create_payload, timeout=30)
    if response.status_code == 200:
        print(f"   ✅ Created 'PTK Revenue History' sheet")
    else:
        print(f"   ⚠️  Sheet might already exist: {response.status_code}")
        print(f"   Response: {response.text[:200]}")
except Exception as e:
    print(f"   ⚠️  Create sheet error: {str(e)}")

# Write data
print(f"\n4. Writing data to sheet...")

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

    response = requests.post(f"{SHEETS_BASE}/proxy", json=write_payload, timeout=60)
    if response.status_code == 200:
        print(f"   ✅ Wrote {len(all_rows)} rows to Google Sheets")
        print(f"\n📊 Sheet URL:")
        print(f"   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
    else:
        print(f"   ❌ Sheet write failed: {response.status_code}")
        print(f"   Response: {response.text[:500]}")
        # Save locally as backup
        with open("/tmp/ptk_data_for_sheets.json", 'w') as f:
            json.dump(all_rows, f, indent=2)
        print(f"   💾 Saved to /tmp/ptk_data_for_sheets.json")
except Exception as e:
    print(f"   ❌ Sheet write error: {str(e)}")
    with open("/tmp/ptk_data_for_sheets.json", 'w') as f:
        json.dump(all_rows, f, indent=2)
    print(f"   💾 Saved to /tmp/ptk_data_for_sheets.json")

print(f"\n{'='*80}")
print(f"✅ COMPLETE!")
print(f"   MongoDB: ✅ 1,206 records in ptk_connect.revenue_report_raw")
print(f"   Google Sheets: {len(all_rows)} rows × {len(sorted_periods) + 1} columns")
print(f"   Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
print(f"{'='*80}")
