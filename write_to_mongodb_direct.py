#!/usr/bin/env python3
"""
Write PTK data directly to MongoDB Atlas (bypassing Calendar GPT proxy)
"""

import json
import os
from datetime import datetime
from pymongo import MongoClient

# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://evolution_admin:th5ozXlvZzgoeBRb@cluster0.irfuesb.mongodb.net/?retryWrites=true&w=majority")
DATA_FILE = "/tmp/ptk_all_historical_data_20251125_202907.json"

print("="*80)
print("Writing PTK Historical Data Directly to MongoDB")
print("="*80)

# Load data
print(f"\n1. Loading data from {DATA_FILE}...")
with open(DATA_FILE, 'r') as f:
    all_records = json.load(f)

print(f"   ✅ Loaded {len(all_records)} records")

# Connect to MongoDB
print(f"\n2. Connecting to MongoDB Atlas...")
try:
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    # Test connection
    client.admin.command('ping')
    print(f"   ✅ Connected to MongoDB Atlas")
except Exception as e:
    print(f"   ❌ Connection failed: {str(e)}")
    exit(1)

# Select database and collection
db = client["ptk_connect"]
collection = db["revenue_report_raw"]

print(f"\n3. Database: {db.name}")
print(f"   Collection: {collection.name}")

# Clear existing data (optional - comment out if you want to append)
print(f"\n4. Clearing existing data...")
delete_result = collection.delete_many({})
print(f"   ✅ Deleted {delete_result.deleted_count} existing records")

# Insert new data
print(f"\n5. Inserting {len(all_records)} new records...")
try:
    insert_result = collection.insert_many(all_records, ordered=False)
    print(f"   ✅ Inserted {len(insert_result.inserted_ids)} records")
except Exception as e:
    print(f"   ⚠️  Insert error: {str(e)}")
    print(f"   (Some records may have been inserted)")

# Verify insertion
print(f"\n6. Verifying data...")
count = collection.count_documents({})
print(f"   ✅ Total records in collection: {count}")

# Show sample record
sample = collection.find_one()
if sample:
    print(f"\n7. Sample record:")
    print(f"   Metric: {sample.get('', 'N/A')}")
    print(f"   Start From: {sample.get('_start_from', 'N/A')}")
    print(f"   Extracted At: {sample.get('_extracted_at', 'N/A')}")
    # Show first 5 time period columns
    time_periods = [k for k in sample.keys() if k not in ['', '_start_from', '_extracted_at', '_id']][:5]
    for period in time_periods:
        print(f"   {period}: {sample.get(period, 'N/A')}")

# Get unique metrics
print(f"\n8. Analyzing data...")
unique_metrics = collection.distinct("")
print(f"   ✅ Unique metrics: {len(unique_metrics)}")
print(f"   Sample metrics: {unique_metrics[:10]}")

# Get unique time periods
unique_periods = collection.distinct("_start_from")
print(f"   ✅ Unique time periods: {unique_periods}")

print(f"\n{'='*80}")
print(f"✅ MONGODB WRITE COMPLETE!")
print(f"   Database: ptk_connect")
print(f"   Collection: revenue_report_raw")
print(f"   Records: {count}")
print(f"   Metrics: {len(unique_metrics)}")
print(f"{'='*80}")

# Close connection
client.close()
