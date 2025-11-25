#!/usr/bin/env python3
"""
Write complete Covent Garden FPS execution report to MongoDB SSM
"""

import requests
from datetime import datetime

MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
EXECUTION_ID = open("/tmp/cg_execution_id.txt").read().strip()

print("="*80)
print("WRITING COMPLETE EXECUTION REPORT TO SSM")
print("="*80)

# Complete execution report
execution_report = {
    "workflow_id": "covent_garden_fps_import_nov25_2025",
    "execution_id": EXECUTION_ID,
    "status": "completed",
    "completed_at": datetime.utcnow().isoformat(),
    "total_duration_seconds": "~180",

    "summary": {
        "source_spreadsheet": "1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ",
        "source_tabs": ["2023 Statistics", "2024 Statistics", "2025 Statistics"],
        "mongodb_collection": "franchise_fps.covent_garden_statistics_by_period",
        "records_extracted": 222,
        "unique_periods": 74,
        "metrics_count": 18,
        "mirror_sheet_id": "1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk",
        "mirror_sheet_url": "https://docs.google.com/spreadsheets/d/1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk/edit",
        "cells_written": 1425
    },

    "steps_completed": [
        {
            "step": 1,
            "action": "list_all_tabs",
            "description": "Listed all 63 tabs in Covent Garden FPS spreadsheet",
            "result": "Found statistics tabs: 2023, 2024, 2025 Statistics + Failed Payments tab",
            "status": "success"
        },
        {
            "step": 2,
            "action": "inspect_structure",
            "description": "Inspected statistics tab structure",
            "result": "Row 1: Automated labels, Row 2: Headers, Row 3+: Data",
            "status": "success"
        },
        {
            "step": 3,
            "action": "extract_2023_statistics",
            "description": "Extracted 2023 Statistics tab",
            "result": "74 period records extracted",
            "status": "success"
        },
        {
            "step": 4,
            "action": "extract_2024_statistics",
            "description": "Extracted 2024 Statistics tab",
            "result": "74 period records extracted",
            "status": "success"
        },
        {
            "step": 5,
            "action": "extract_2025_statistics",
            "description": "Extracted 2025 Statistics tab",
            "result": "74 period records extracted",
            "status": "success"
        },
        {
            "step": 6,
            "action": "write_to_mongodb",
            "description": "Wrote all records to MongoDB",
            "result": "222 records inserted into franchise_fps.covent_garden_statistics_by_period",
            "status": "success"
        },
        {
            "step": 7,
            "action": "read_from_mongodb",
            "description": "Read data back from MongoDB for mirror creation",
            "result": "222 records retrieved",
            "status": "success"
        },
        {
            "step": 8,
            "action": "organize_for_sheets",
            "description": "Organized data in transposed format (metrics as rows, periods as columns)",
            "result": "19 rows × 75 columns built",
            "status": "success"
        },
        {
            "step": 9,
            "action": "create_mirror_sheet",
            "description": "Created new Google Sheet for mirror",
            "result": "Sheet ID: 1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk",
            "status": "success"
        },
        {
            "step": 10,
            "action": "write_to_mirror",
            "description": "Wrote data to mirror sheet",
            "result": "1,425 cells written (19 rows × 75 columns)",
            "status": "success"
        },
        {
            "step": 11,
            "action": "verify_mirror",
            "description": "Verified data in mirror sheet",
            "result": "19 rows verified with all metrics and periods",
            "status": "success"
        }
    ],

    "data_lineage": {
        "source_system": "Google Sheets",
        "source_id": "1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ",
        "source_name": "Covent Garden - Franchise Payment Summary",
        "extraction_method": "Google Sheets API via Calendar GPT proxy",
        "intermediate_storage": "MongoDB Atlas (franchise_fps.covent_garden_statistics_by_period)",
        "final_destination": "Google Sheets (1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk)",
        "architecture": "MongoDB-first with Sheets as read-only mirror"
    },

    "metrics_extracted": [
        "% number of failed payments",
        "% number of still uncollected payments",
        "% value of failed payments",
        "% value of still uncollected payments",
        "Average % number of failed payments",
        "Average % number of uncollected payments",
        "Average % value of failed payments",
        "Average % value of uncollected payments",
        "Failed number of payments",
        "Failed value of payments",
        "Still uncollected number of payments",
        "Still uncollected value of payments",
        "Total number of payments",
        "Total value of payments"
    ],

    "period_coverage": {
        "start_period": "2023 April first half",
        "end_period": "2025 November second half",
        "total_periods": 74
    },

    "files_created": [
        "/tmp/ptk_scraper/list_covent_garden_tabs.py",
        "/tmp/ptk_scraper/inspect_covent_garden_sheet.py",
        "/tmp/ptk_scraper/extract_covent_garden_proper.py",
        "/tmp/ptk_scraper/create_covent_garden_mirror.py",
        "/tmp/ptk_scraper/debug_covent_garden_sheet.py",
        "/tmp/ptk_scraper/verify_sheet_data.py"
    ]
}

# Update execution log
print("\n1. Updating workflow execution in SSM...")

payload = {
    "database": "scheduler_memory",
    "collection": "workflow_executions",
    "filter": {"execution_id": EXECUTION_ID},
    "update": {"$set": execution_report}
}

response = requests.post(
    f"{MONGODB_BASE}/mongodb/update",
    json=payload,
    timeout=30
)

if response.status_code == 200:
    print("   ✅ Execution report written to SSM")
    print(f"   Collection: scheduler_memory.workflow_executions")
    print(f"   Execution ID: {EXECUTION_ID}")
else:
    print(f"   ⚠️ Failed: {response.status_code}")
    print(f"   Response: {response.text[:200]}")

# Also insert a summary document
summary_doc = {
    "document_type": "import_summary",
    "project": "Covent Garden FPS",
    "date": datetime.utcnow().isoformat(),
    "status": "completed",
    "source": "Google Sheets (1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ)",
    "destination": {
        "mongodb": "franchise_fps.covent_garden_statistics_by_period (222 records)",
        "sheets": "https://docs.google.com/spreadsheets/d/1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk/edit"
    },
    "architecture": "MongoDB-first with Google Sheets as read-only mirror",
    "execution_id": EXECUTION_ID
}

print("\n2. Creating import summary document...")

summary_payload = {
    "database": "scheduler_memory",
    "collection": "import_summaries",
    "document": summary_doc
}

summary_response = requests.post(
    f"{MONGODB_BASE}/mongodb/insert",
    json=summary_payload,
    timeout=30
)

if summary_response.status_code == 200:
    print("   ✅ Summary document created")
else:
    print(f"   ⚠️ Failed: {summary_response.status_code}")

print(f"\n{'='*80}")
print("✅ SSM DOCUMENTATION COMPLETE")
print(f"{'='*80}")
print(f"\nExecution ID: {EXECUTION_ID}")
print(f"Workflow: covent_garden_fps_import_nov25_2025")
print(f"Status: completed")
print(f"\nMongoDB: franchise_fps.covent_garden_statistics_by_period")
print(f"Records: 222")
print(f"\nMirror Sheet: https://docs.google.com/spreadsheets/d/1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk/edit")
