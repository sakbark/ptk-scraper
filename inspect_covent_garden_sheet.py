#!/usr/bin/env python3
"""
Inspect Covent Garden sheet structure to see what we should be extracting
"""

import requests

MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
SPREADSHEET_ID = "1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ"

print("="*80)
print("INSPECTING COVENT GARDEN FPS SHEET STRUCTURE")
print("="*80)

# Get one period sheet to see the structure
test_sheet = "2025 September Second Half"

print(f"\n1. Reading sample sheet: {test_sheet}")

payload = {
    "body": {
        "service": "sheets",
        "operation": "spreadsheets.values.get",
        "spreadsheetId": SPREADSHEET_ID,
        "range": f"'{test_sheet}'!A1:Z100"
    },
    "account": "work"
}

response = requests.post(
    f"{MONGODB_BASE}/proxy",
    json=payload,
    timeout=30
)

if response.status_code == 200:
    result = response.json()
    values = result.get('values', [])

    print(f"   ✅ Found {len(values)} rows")

    print(f"\n2. First 20 rows:")
    for i, row in enumerate(values[:20]):
        print(f"   Row {i+1}: {row}")

    # Try to identify the data structure
    print(f"\n3. Data structure analysis:")

    # Count non-empty rows
    non_empty = [row for row in values if any(cell for cell in row)]
    print(f"   Non-empty rows: {len(non_empty)}")

    # Check if there are column headers
    if values:
        print(f"   First row (possible headers): {values[0]}")

        # Check for typical FPS metrics
        fps_metrics = [
            "Total value of payments",
            "Total number of failed payments",
            "Revenue",
            "Costs",
            "VAT",
            "Commission"
        ]

        print(f"\n4. Looking for typical FPS metrics in data:")
        for metric in fps_metrics:
            found = False
            for i, row in enumerate(values):
                if row and metric.lower() in str(row[0]).lower():
                    print(f"   ✅ Found '{metric}' at row {i+1}: {row}")
                    found = True
                    break
            if not found:
                print(f"   ❌ Metric '{metric}' not found")

else:
    print(f"   ❌ Failed to read sheet: {response.status_code}")
    print(f"   Response: {response.text[:500]}")

print(f"\n" + "="*80)
