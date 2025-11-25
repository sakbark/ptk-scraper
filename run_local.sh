#!/bin/bash
# Run PTK Connect Scraper locally

set -e

echo "🚀 Running PTK Connect Scraper locally..."

# Set environment variables
export PTK_USERNAME="Saad.K"
export PTK_PASSWORD="RedDog2020"
export MONGODB_BASE="https://calendar-gpt-958443682078.europe-west2.run.app"
export DATABASE="ptk_connect"

# Check if dependencies are installed
if ! python3 -c "import playwright" 2>/dev/null; then
    echo "📦 Installing dependencies..."
    pip3 install -r requirements.txt
    playwright install chromium
fi

# Run the scraper
echo "🤖 Starting extraction..."
python3 ptk_scraper.py test

echo "✅ Done!"
