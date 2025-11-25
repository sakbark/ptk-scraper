#!/usr/bin/env python3
"""
List all tabs in Covent Garden FPS spreadsheet
"""

import requests

MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
SPREADSHEET_ID = "1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ"

print("="*80)
print("COVENT GARDEN FPS - ALL TABS")
print("="*80)

payload = {
    "body": {
        "service": "sheets",
        "operation": "spreadsheets.get",
        "spreadsheetId": SPREADSHEET_ID
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
    sheets = result.get('sheets', [])

    print(f"\nFound {len(sheets)} sheets:\n")

    # Look for statistics tabs
    statistics_tabs = []

    for i, sheet in enumerate(sheets):
        title = sheet['properties']['title']
        sheet_id = sheet['properties']['sheetId']
        print(f"{i+1}. {title} (ID: {sheet_id})")

        # Check if this is a statistics/data tab
        if any(keyword in title.lower() for keyword in ['statistics', 'revenue', 'failed', 'cost', 'payment']):
            statistics_tabs.append(title)

    print(f"\n{'='*80}")
    print(f"STATISTICS TABS FOUND: {len(statistics_tabs)}")
    print(f"{'='*80}")
    for tab in statistics_tabs:
        print(f"  • {tab}")

else:
    print(f"Failed: {response.status_code}")
    print(response.text[:500])
