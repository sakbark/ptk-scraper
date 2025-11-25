# PTK Connect Scraper

Automated data extraction from PTK Connect using Playwright browser automation.

## Features

- ✅ **Automated Login**: Handles PTK Connect authentication
- ✅ **Report Download**: Clicks "Download Report" and saves file
- ✅ **Data Parsing**: Reads CSV/Excel files with pandas
- ✅ **MongoDB Integration**: Pushes data directly to MongoDB
- ✅ **Cloud Run Ready**: Deploys as serverless microservice
- ✅ **Extensible**: Easy to add more report types

## Architecture

```
PTK Connect Scraper (Cloud Run)
├── Playwright Browser (Chromium)
├── Download Manager
├── Pandas Parser
└── MongoDB Client → Calendar GPT Proxy
```

## Quick Start

### Local Testing

```bash
cd /tmp/ptk_scraper

# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Set credentials
export PTK_USERNAME="your_username"
export PTK_PASSWORD="your_password"

# Test login
python ptk_scraper.py test

# Or run as API
python ptk_scraper.py
```

### Deploy to Cloud Run

```bash
# 1. Store credentials in Google Secret Manager
echo -n "your_username" | gcloud secrets create ptk_username --data-file=-
echo -n "your_password" | gcloud secrets create ptk_password --data-file=-

# 2. Deploy
bash deploy.sh
```

## API Endpoints

### Health Check
```bash
GET /health
```

### Extract Revenue Report
```bash
POST /extract/revenue

# Optional: Override credentials
{
  "username": "...",
  "password": "...",
  "start_period": "January 2020"
}
```

### Test Login
```bash
POST /extract/test-login

{
  "username": "...",
  "password": "..."
}
```

## Usage Examples

### Manual Trigger
```bash
# Trigger extraction
curl -X POST https://ptk-connect-scraper-XXXXX.run.app/extract/revenue

# Response:
{
  "status": "success",
  "records_extracted": 500,
  "mongodb_summary": {
    "revenue_by_location": {"inserted": 500}
  },
  "duration_seconds": 45.2
}
```

### Scheduled Extraction (Cloud Scheduler)
```bash
# Set up daily extraction at 6am
gcloud scheduler jobs create http ptk-revenue-daily \
  --schedule="0 6 * * *" \
  --uri="https://ptk-connect-scraper-XXXXX.run.app/extract/revenue" \
  --http-method=POST \
  --location=europe-west2
```

## Data Flow

```
1. PTK Connect Website
   ↓ Playwright Login
2. Navigate to Revenue Report
   ↓ Click "Download Report"
3. Downloaded CSV/Excel File
   ↓ Pandas Parse
4. Structured Data
   ↓ Transform & Clean
5. MongoDB (ptk_connect database)
   ↓ Query & Analyze
6. Your Applications
```

## MongoDB Schema

Revenue data is stored in: `ptk_connect.revenue_by_location`

```json
{
  "location": "London/Atkinson",
  "Jan": 123,
  "Feb": 456,
  "Mar": 789,
  "extracted_at": "2025-11-25T20:00:00Z",
  "source": "ptk_connect_revenue_report"
}
```

## Adding More Reports

To add extraction for other PTK Connect reports:

1. Add new method to `PTKConnectScraper`:
```python
def download_cost_report(self):
    self.page.goto("https://ptkconnect.com/reports/cost-report/")
    # ... download logic
```

2. Add new API endpoint:
```python
@app.route('/extract/costs', methods=['POST'])
def extract_costs():
    with PTKConnectScraper() as scraper:
        scraper.login()
        file_path = scraper.download_cost_report()
        # ... parse and save
```

3. Deploy updated service

## Troubleshooting

**Login fails**:
- Check credentials in Google Secret Manager
- Verify PTK Connect website is accessible
- Check logs: `gcloud run logs read ptk-connect-scraper`

**Download fails**:
- Verify "Download Report" button selector
- Check screenshot saved to `/tmp/error_screenshot.png`
- Website structure may have changed

**Parse fails**:
- Check downloaded file format (CSV vs Excel)
- Verify column names match expectations
- Update parsing logic in `parse_revenue_report()`

## Security

- ✅ Credentials stored in Google Secret Manager (not in code)
- ✅ HTTPS only communication
- ✅ No credential logging
- ✅ Temporary file cleanup
- ✅ Cloud Run IAM authentication (optional)

## Performance

- **Cold start**: ~15-20 seconds (Playwright browser initialization)
- **Warm execution**: ~10-15 seconds per report
- **Memory**: 2GB (handles large downloads)
- **Timeout**: 10 minutes (for large reports)

## Cost Estimate

- **Cloud Run**: ~$0.01 per extraction (10-15 seconds execution)
- **Cloud Scheduler**: $0.10/month for daily job
- **Total**: ~$0.40/month for daily extractions

## Next Steps

1. Deploy service
2. Test with your credentials
3. Verify data in MongoDB
4. Set up Cloud Scheduler for automatic runs
5. Add more report types as needed

## Files

- `ptk_scraper.py` - Main service code
- `requirements.txt` - Python dependencies
- `Dockerfile` - Container configuration
- `deploy.sh` - Deployment script
- `.env.template` - Environment variables template
