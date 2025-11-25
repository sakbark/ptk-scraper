#!/bin/bash
# Deploy PTK Connect Scraper to Cloud Run

set -e

PROJECT_ID="calendar-gpt-958443682078"
SERVICE_NAME="ptk-connect-scraper"
REGION="europe-west2"

echo "🚀 Deploying PTK Connect Scraper to Cloud Run..."

# Build and deploy with credentials as env vars
gcloud run deploy $SERVICE_NAME \
  --source . \
  --platform managed \
  --region $REGION \
  --project $PROJECT_ID \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 600 \
  --set-env-vars MONGODB_BASE=https://calendar-gpt-958443682078.europe-west2.run.app \
  --set-env-vars DATABASE=ptk_connect \
  --set-env-vars PTK_USERNAME=Saad.K \
  --set-env-vars PTK_PASSWORD=RedDog2020

echo "✅ Deployment complete!"
