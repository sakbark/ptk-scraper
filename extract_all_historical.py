#!/usr/bin/env python3
"""
Extract ALL historical PTK Connect data by iterating through date ranges
"""

import json
import time
from datetime import datetime
from ptk_scraper import PTKConnectScraper

# Date ranges to extract (YYYY-MM format)
DATE_RANGES = [
    "2020-01",  # Start from 2020
    "2021-01",  # 2021 data
    "2022-01",  # 2022 data
    "2023-01",  # 2023 data (requested)
    "2024-01",  # 2024 data
    None        # Current/latest data (no parameter)
]

def extract_all_historical_data():
    """Extract data from all historical periods"""

    all_data = []

    with PTKConnectScraper() as scraper:
        # Login once
        if not scraper.login():
            print("❌ Login failed!")
            return

        print(f"✅ Logged in successfully!\n")

        # Extract from each date range
        for i, start_from in enumerate(DATE_RANGES, 1):
            period_label = start_from if start_from else "Latest"
            print(f"[{i}/{len(DATE_RANGES)}] Extracting from {period_label}...")

            try:
                # Scrape the data
                records = scraper.scrape_revenue_report_table(start_from=start_from)

                if records:
                    # Tag each record with the time period
                    for record in records:
                        record['_start_from'] = start_from or "latest"
                        record['_extracted_at'] = datetime.utcnow().isoformat()

                    all_data.extend(records)
                    print(f"   ✅ Extracted {len(records)} records from {period_label}")
                else:
                    print(f"   ⚠️  No data found for {period_label}")

                # Wait between requests to be polite
                if i < len(DATE_RANGES):
                    time.sleep(2)

            except Exception as e:
                print(f"   ❌ Error extracting {period_label}: {str(e)}")
                continue

    # Save all data
    output_file = f"/tmp/ptk_all_historical_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(all_data, f, indent=2)

    print(f"\n{'='*80}")
    print(f"✅ COMPLETE!")
    print(f"   Total records extracted: {len(all_data)}")
    print(f"   Unique time periods: {len(set(r['_start_from'] for r in all_data))}")
    print(f"   Saved to: {output_file}")
    print(f"{'='*80}")

    return all_data

if __name__ == "__main__":
    print("🚀 PTK Connect - Historical Data Extraction")
    print("="*80)
    print(f"Extracting from {len(DATE_RANGES)} time periods...")
    print("="*80 + "\n")

    data = extract_all_historical_data()

    if data:
        print(f"\n✅ SUCCESS - {len(data)} total records extracted")
        print(f"\nSample record:")
        print(json.dumps(data[0], indent=2))
    else:
        print("\n❌ No data extracted")
