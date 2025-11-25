# PTK Scraper & FPS Data Pipeline

MongoDB-first data pipeline for PTK Connect revenue reports and Franchise Payment Summary (FPS) data.

## 📊 Projects

### 1. PTK Revenue Report Import
**Source**: PTK Connect (https://ptkconnect.com/reports/revenue-report/)
**Method**: Playwright browser automation + BeautifulSoup
**MongoDB**: `ptk_connect.revenue_report_raw` (201 records)
**Mirror Sheet**: [PTK Revenue Report - Latest](https://docs.google.com/spreadsheets/d/1XcZ-xviGNQqhvBQ3UVS9U70TpboNu0t0pxUMMZ2v1Mk/edit)
**Duration**: ~16 seconds
**SSM Execution**: `exec_ptk_20251125_193000`

### 2. Covent Garden FPS Import
**Source**: [Covent Garden FPS Spreadsheet](https://docs.google.com/spreadsheets/d/1weMN0nLbjE0CWpYJO6J9yPg4gH8dx9Ts4MbBaBFQWZQ/edit)
**Method**: Google Sheets API via Calendar GPT
**MongoDB**: `franchise_fps.covent_garden_statistics_by_period` (222 records)
**Mirror Sheet**: [Covent Garden FPS - Mirror](https://docs.google.com/spreadsheets/d/1rGM8K_GJCZr_hY8xU81Nw4JnyhxkvTO63DnGESKkXHk/edit)
**Period Coverage**: 74 periods (2023 April → 2025 November)
**SSM Execution**: `exec_20251125_210229`

### 3. Islington FPS Import
**Source**: Islington FPS Spreadsheet
**MongoDB**: `franchise_fps.*_by_period` (432 records)
**Mirror Sheet**: [Islington FPS - All Data](https://docs.google.com/spreadsheets/d/1W1F3FUk3LYPczVk3pMJAyDJKO-krC6dQHJQulR9bzLE/edit)

## 🏗️ Architecture

**MongoDB-First Approach**:
1. Extract data from source (web scraping or Google Sheets API)
2. Write to MongoDB Atlas as source of truth
3. Read from MongoDB and transform for visualization
4. Create read-only mirror in Google Sheets

**Benefits**:
- Single source of truth in MongoDB
- Data lineage tracking
- Easy querying and aggregation
- Sheets as visualization layer only
- Automated refresh capability

## 📂 Key Scripts

### PTK Connect
- `ptk_scraper.py` - Main PTK scraper (Playwright + BeautifulSoup)
- `write_to_mongodb_direct.py` - Direct MongoDB writer
- `write_to_sheets_direct.py` - Sheets export via Calendar GPT
- `write_ptk_execution_to_ssm.py` - SSM documentation

### Covent Garden FPS
- `list_covent_garden_tabs.py` - Tab discovery
- `extract_covent_garden_proper.py` - Statistics extraction
- `create_covent_garden_mirror.py` - Mirror sheet creation
- `write_execution_report_to_ssm.py` - SSM documentation

### Utilities
- `debug_covent_garden_sheet.py` - Debugging helper
- `verify_sheet_data.py` - Data verification
- `inspect_covent_garden_sheet.py` - Structure inspection

## 🚀 Quick Start

### PTK Revenue Report
```bash
# Set credentials
export PTK_USERNAME="Saad.K"
export PTK_PASSWORD="RedDog2020"
export MONGODB_URI="mongodb+srv://..."

# Run scraper
python3 ptk_scraper.py test
```

### Covent Garden FPS
```bash
# Extract from Google Sheets to MongoDB
python3 extract_covent_garden_proper.py

# Create mirror sheet
python3 create_covent_garden_mirror.py

# Document to SSM
python3 write_execution_report_to_ssm.py
```

## 🔧 Environment Variables

- `PTK_USERNAME` - PTK Connect username
- `PTK_PASSWORD` - PTK Connect password
- `MONGODB_URI` - MongoDB Atlas connection string
- `MONGODB_BASE` - Calendar GPT API base URL (default: https://calendar-gpt-958443682078.europe-west2.run.app)
- `DATABASE` - MongoDB database name

## 📊 MongoDB Collections

### PTK Connect Database
- `revenue_report_raw` - Latest PTK revenue metrics

### Franchise FPS Database
- `covent_garden_statistics_by_period` - Covent Garden FPS data (222 records)
- `revenue_statistics_by_period` - Islington revenue data
- `failed_payments_by_period` - Islington failed payments
- `cost_details_by_period` - Islington cost details

### Scheduler Memory Database
- `workflows` - Workflow definitions
- `workflow_executions` - Execution logs
- `import_summaries` - Import summaries
- `data_lineage_map` - Data lineage tracking

## 📈 Data Lineage

All records include metadata:
- `source_spreadsheet` / `_source_url` - Origin
- `extraction_date` / `_extracted_at` - Timestamp
- `data_type` - Classification
- `source_tab` - Sheet name (if applicable)

Query example:
```javascript
// Find where a specific value came from
db.covent_garden_statistics_by_period.findOne({
  "Period": "2023 November second half",
  "Total value of payments": "£8,092.91"
})
```

## 🔄 Deployment

```bash
# Deploy PTK scraper to Cloud Run
./deploy.sh

# Runs as scheduled job or on-demand
```

## 📝 SSM Documentation

All workflows documented in MongoDB SSM:
- Workflow plans before execution
- Step-by-step execution logs
- Import summaries with results
- Data lineage records

**Query SSM**:
```javascript
// View all workflows
db.workflows.find()

// View execution logs
db.workflow_executions.find().sort({completed_at: -1})

// View import summaries
db.import_summaries.find().sort({date: -1})
```

## ✅ Success Metrics

### PTK Import
- ✅ 201 metrics extracted
- ✅ 16.3 second execution time
- ✅ MongoDB-first architecture
- ✅ Mirror sheet created

### Covent Garden FPS
- ✅ 222 period records extracted
- ✅ 74 unique periods
- ✅ 18 financial metrics
- ✅ 1,425 cells in mirror sheet

### Islington FPS
- ✅ 432 records across 3 collections
- ✅ ~144 periods
- ✅ Full data lineage tracking

## 🔗 Links

- **GitHub**: https://github.com/sakbark/ptk-scraper
- **Calendar GPT**: https://calendar-gpt-958443682078.europe-west2.run.app
- **PTK Connect**: https://ptkconnect.com/reports/revenue-report/

---

**Created**: November 25, 2025
**Architecture**: MongoDB-first with Google Sheets as read-only mirror
**Maintained by**: Claude Code
