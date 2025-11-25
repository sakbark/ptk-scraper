#!/usr/bin/env python3
"""
Verify what's actually in the Covent Garden mirror sheet
"""

import requests

MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
SHEET_ID = "1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk"

print("="*80)
print("VERIFYING COVENT GARDEN MIRROR SHEET CONTENTS")
print("="*80)

# Read entire sheet
payload = {
    "body": {
        "service": "sheets",
        "operation": "spreadsheets.values.get",
        "spreadsheetId": SHEET_ID,
        "range": "Sheet1"
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

    print(f"\n✅ Sheet read successfully")
    print(f"Total rows: {len(values)}")

    if len(values) == 0:
        print("\n❌ SHEET IS EMPTY!")
    else:
        print(f"\nFirst 10 rows:")
        for i, row in enumerate(values[:10]):
            print(f"  Row {i+1}: {row[:5] if len(row) > 5 else row}... (showing first 5 cells)")

        print(f"\nLast row:")
        print(f"  Row {len(values)}: {values[-1][:5] if len(values[-1]) > 5 else values[-1]}...")

        # Check if all rows are empty
        non_empty_rows = [row for row in values if any(cell for cell in row)]
        print(f"\nNon-empty rows: {len(non_empty_rows)}")
else:
    print(f"❌ Failed to read sheet: {response.status_code}")
    print(f"Response: {response.text[:500]}")

print(f"\n{'='*80}")
print(f"Sheet URL: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")
