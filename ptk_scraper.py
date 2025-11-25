#!/usr/bin/env python3
"""
PTK Connect Scraper Service
============================

Automated data extraction from PTK Connect using Playwright.
Logs in, downloads reports, extracts data, pushes to MongoDB.

Features:
- Persistent authentication with session management
- Multiple report downloaders (revenue, costs, payments, etc.)
- Direct MongoDB integration
- Scheduled execution via Cloud Scheduler
- Error handling and retry logic
"""

import os
import time
import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from flask import Flask, request, jsonify
from playwright.sync_api import sync_playwright, Browser, Page, BrowserContext
import requests
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ==================== Configuration ====================

PTK_CONNECT_URL = "https://ptkconnect.com"
REVENUE_REPORT_BASE_URL = "https://ptkconnect.com/reports/revenue-report/"
MONGODB_BASE = "https://calendar-gpt-958443682078.europe-west2.run.app"
DATABASE = "ptk_connect"

# Credentials (will be loaded from Google Secret Manager in production)
PTK_USERNAME = os.getenv("PTK_USERNAME")
PTK_PASSWORD = os.getenv("PTK_PASSWORD")

# ==================== Browser Automation ====================

class PTKConnectScraper:
    """Handles all PTK Connect automation and data extraction"""

    def __init__(self):
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.download_dir = tempfile.mkdtemp()
        logger.info(f"Download directory: {self.download_dir}")

    def __enter__(self):
        """Context manager entry - start browser"""
        self.start_browser()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup"""
        self.close_browser()

    def start_browser(self):
        """Start Playwright browser with download settings"""
        logger.info("Starting Playwright browser...")

        playwright = sync_playwright().start()
        self.browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu'
            ]
        )

        # Create context with download handling
        self.context = self.browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080}
        )

        self.page = self.context.new_page()
        logger.info("Browser started successfully")

    def close_browser(self):
        """Close browser and cleanup"""
        if self.page:
            self.page.close()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        logger.info("Browser closed")

    def login(self, username: str = None, password: str = None) -> bool:
        """
        Login to PTK Connect

        Args:
            username: PTK Connect username
            password: PTK Connect password

        Returns:
            True if login successful, False otherwise
        """
        username = username or PTK_USERNAME
        password = password or PTK_PASSWORD

        if not username or not password:
            logger.error("Missing PTK Connect credentials")
            return False

        try:
            logger.info(f"Navigating to PTK Connect login page...")
            self.page.goto(PTK_CONNECT_URL, wait_until="networkidle")

            # Check if already logged in (might redirect away from login)
            if "login" not in self.page.url.lower():
                logger.info("Already logged in!")
                return True

            # Fill login form
            logger.info("Filling login form...")
            self.page.fill('input[name="username"]', username)
            self.page.fill('input[name="password"]', password)

            # Submit form
            logger.info("Submitting login form...")
            self.page.click('button[type="submit"]')

            # Wait for navigation after login
            self.page.wait_for_load_state("networkidle")

            # Check if login successful
            if "login" in self.page.url.lower():
                logger.error("Login failed - still on login page")
                return False

            logger.info(f"Login successful! Current URL: {self.page.url}")
            return True

        except Exception as e:
            logger.error(f"Login error: {str(e)}", exc_info=True)
            return False

    def scrape_revenue_report_table(self, start_from: str = None) -> List[Dict[str, Any]]:
        """
        Scrape revenue report table directly from HTML

        Args:
            start_from: Start date in YYYY-MM format (e.g., "2023-01")

        Returns:
            List of records extracted from the table
        """
        try:
            # Build URL with optional start date
            url = REVENUE_REPORT_BASE_URL
            if start_from:
                url = f"{url}?startFrom={start_from}"
                logger.info(f"Navigating to revenue report page (from {start_from})...")
            else:
                logger.info(f"Navigating to revenue report page...")

            self.page.goto(url, wait_until="networkidle")

            # Wait for table to load
            time.sleep(3)

            # Try to click "Compressed Version" to get more data
            try:
                compressed_btn = self.page.locator('text="Compressed Version"')
                if compressed_btn.is_visible():
                    logger.info("Clicking 'Compressed Version' button...")
                    compressed_btn.click()
                    time.sleep(2)
            except:
                logger.info("No 'Compressed Version' button found, continuing with current view...")

            # Get the page HTML
            html_content = self.page.content()

            # Parse with BeautifulSoup
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all tables
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables on page")

            if not tables:
                logger.error("No tables found on page")
                screenshot_path = f"{self.download_dir}/no_tables_screenshot.png"
                self.page.screenshot(path=screenshot_path)
                return []

            # Extract data from the main table
            all_records = []

            # Get all rows from all tables
            for table_idx, table in enumerate(tables):
                rows = table.find_all('tr')
                logger.info(f"Table {table_idx}: {len(rows)} rows")

                # Extract headers and data
                headers = []
                for row in rows:
                    cells = row.find_all(['th', 'td'])
                    row_data = [cell.get_text(strip=True) for cell in cells]

                    if not headers and row_data:
                        headers = row_data
                        logger.info(f"Headers: {headers[:10]}...")  # First 10 headers
                    elif row_data:
                        # Create record
                        record = {}
                        for i, value in enumerate(row_data):
                            if i < len(headers):
                                record[headers[i]] = value
                        if record:
                            all_records.append(record)

            logger.info(f"✅ Scraped {len(all_records)} records from HTML tables")
            return all_records

        except Exception as e:
            logger.error(f"Scrape error: {str(e)}", exc_info=True)
            try:
                screenshot_path = f"{self.download_dir}/error_screenshot.png"
                self.page.screenshot(path=screenshot_path)
                logger.info(f"Screenshot saved: {screenshot_path}")
            except:
                pass
            return []

    def parse_revenue_report(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse downloaded revenue report file

        Args:
            file_path: Path to downloaded CSV/Excel file

        Returns:
            List of records extracted from the file
        """
        try:
            logger.info(f"Parsing revenue report: {file_path}")

            # Detect file type and read accordingly
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                logger.error(f"Unsupported file type: {file_path}")
                return []

            logger.info(f"File loaded: {len(df)} rows × {len(df.columns)} columns")
            logger.info(f"Columns: {list(df.columns)}")

            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')

            logger.info(f"✅ Parsed {len(records)} records from revenue report")
            return records

        except Exception as e:
            logger.error(f"Parse error: {str(e)}", exc_info=True)
            return []

    def extract_and_transform(self, records: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Transform raw records into structured format for MongoDB

        Args:
            records: Raw records from CSV/Excel

        Returns:
            Dictionary with categorized data ready for MongoDB
        """
        try:
            logger.info(f"Transforming {len(records)} records...")

            # This will be customized based on actual data structure
            # For now, just organize by location/listing

            transformed = {
                "by_location": [],
                "by_month": [],
                "totals": []
            }

            # Example transformation (will be adjusted based on actual data)
            for record in records:
                # Clean up record - remove NaN, convert types
                clean_record = {}
                for key, value in record.items():
                    if pd.notna(value):
                        clean_record[key] = value

                # Add metadata
                clean_record["extracted_at"] = datetime.utcnow().isoformat()
                clean_record["source"] = "ptk_connect_revenue_report"

                transformed["by_location"].append(clean_record)

            logger.info(f"✅ Transformed data ready for MongoDB")
            return transformed

        except Exception as e:
            logger.error(f"Transform error: {str(e)}", exc_info=True)
            return {}

    def push_to_mongodb(self, data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        Push extracted data to MongoDB

        Args:
            data: Transformed data dictionary

        Returns:
            Summary of inserted records
        """
        try:
            logger.info("Pushing data to MongoDB...")

            summary = {}

            for collection_name, records in data.items():
                if not records:
                    continue

                # Insert into MongoDB
                collection = f"revenue_{collection_name}"
                result = self._mongodb_insert(collection, records)
                summary[collection] = result

            logger.info(f"✅ Data pushed to MongoDB: {summary}")
            return summary

        except Exception as e:
            logger.error(f"MongoDB push error: {str(e)}", exc_info=True)
            return {}

    def _mongodb_insert(self, collection: str, records: List[Dict]) -> Dict:
        """Insert records into MongoDB via Calendar GPT proxy"""
        # Use the correct endpoint format from FPS system
        url = "https://calendar-gpt-958443682078.europe-west2.run.app/mongodb/insertMany"

        payload = {
            "database": DATABASE,
            "collection": collection,
            "documents": records
        }

        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Inserted {len(records)} records into {collection}")
            return {
                "inserted": len(records),
                "collection": collection
            }
        except requests.exceptions.HTTPError as e:
            # Try alternate endpoint
            alt_url = "https://calendar-gpt-958443682078.us-central1.run.app/mongodb/insertMany"
            logger.info(f"Trying alternate endpoint: {alt_url}")
            response = requests.post(alt_url, json=payload, timeout=30)
            response.raise_for_status()
            logger.info(f"✅ Inserted {len(records)} records into {collection}")
            return {
                "inserted": len(records),
                "collection": collection
            }

    def run_full_extraction(self) -> Dict[str, Any]:
        """
        Run complete extraction workflow:
        1. Login
        2. Download revenue report
        3. Parse data
        4. Transform data
        5. Push to MongoDB

        Returns:
            Summary of extraction results
        """
        start_time = datetime.utcnow()
        logger.info("=" * 80)
        logger.info("Starting PTK Connect full extraction...")
        logger.info("=" * 80)

        try:
            # Step 1: Login
            if not self.login():
                return {
                    "status": "error",
                    "message": "Login failed",
                    "timestamp": start_time.isoformat()
                }

            # Step 2: Scrape revenue report table
            records = self.scrape_revenue_report_table()
            if not records:
                return {
                    "status": "error",
                    "message": "No data parsed from file",
                    "timestamp": start_time.isoformat()
                }

            # Step 4: Transform data
            transformed_data = self.extract_and_transform(records)

            # Step 5: Push to MongoDB
            mongodb_summary = self.push_to_mongodb(transformed_data)

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "records_extracted": len(records),
                "mongodb_summary": mongodb_summary,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }

            logger.info("=" * 80)
            logger.info(f"✅ Extraction complete: {len(records)} records in {duration:.2f}s")
            logger.info("=" * 80)

            return result

        except Exception as e:
            logger.error(f"Extraction error: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }


# ==================== Flask API ====================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "ptk-connect-scraper",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat()
    })


@app.route('/extract/revenue', methods=['POST'])
def extract_revenue():
    """
    Extract revenue report data

    POST body (optional):
    {
        "username": "...",
        "password": "...",
        "start_period": "January 2020"
    }
    """
    try:
        data = request.json or {}
        username = data.get("username")
        password = data.get("password")

        # Override environment credentials if provided
        if username:
            os.environ["PTK_USERNAME"] = username
        if password:
            os.environ["PTK_PASSWORD"] = password

        with PTKConnectScraper() as scraper:
            result = scraper.run_full_extraction()

        return jsonify(result), 200 if result["status"] == "success" else 500

    except Exception as e:
        logger.error(f"API error: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500


@app.route('/extract/test-login', methods=['POST'])
def test_login():
    """Test login to PTK Connect"""
    try:
        data = request.json or {}
        username = data.get("username")
        password = data.get("password")

        if username:
            os.environ["PTK_USERNAME"] = username
        if password:
            os.environ["PTK_PASSWORD"] = password

        with PTKConnectScraper() as scraper:
            success = scraper.login()

            return jsonify({
                "status": "success" if success else "failed",
                "message": "Login successful" if success else "Login failed",
                "timestamp": datetime.utcnow().isoformat()
            })

    except Exception as e:
        logger.error(f"Test login error: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500


# ==================== Main ====================

if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Test mode - run extraction locally
        print("Running PTK Connect scraper in test mode...")
        with PTKConnectScraper() as scraper:
            result = scraper.run_full_extraction()
            print(json.dumps(result, indent=2))
    else:
        # Production mode - run Flask server
        port = int(os.environ.get('PORT', 8080))
        app.run(host='0.0.0.0', port=port, debug=False)
