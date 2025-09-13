#!/bin/bash

# Mammoth Batch Manager Startup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Mammoth Batch Manager...${NC}"

# Check Python installation
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source venv/bin/activate

# Install/upgrade dependencies
echo -e "${YELLOW}Installing dependencies...${NC}"
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Configuration
BATCH_API_URL=${BATCH_API_URL:-"http://localhost:8000"}
S3_BUCKET_NAME=${S3_BUCKET_NAME:-"modular-batch-api-batches"}
PORT=${PORT:-8080}

echo -e "${GREEN}Configuration:${NC}"
echo "  Batch API URL: $BATCH_API_URL"
echo "  S3 Bucket: $S3_BUCKET_NAME"
echo "  Web UI Port: $PORT"

# Check if batch API is reachable
echo -e "${YELLOW}Checking Batch API connectivity...${NC}"
if curl -s -o /dev/null -w "%{http_code}" "$BATCH_API_URL/health" | grep -q "200"; then
    echo -e "${GREEN}✓ Batch API is reachable${NC}"
else
    echo -e "${YELLOW}⚠ Warning: Batch API at $BATCH_API_URL is not reachable${NC}"
    echo -e "${YELLOW}  The UI will start but batch operations may fail${NC}"
fi

# AWS credentials check removed for now - can be added manually in UI

# Start the application
echo -e "${GREEN}Starting web server on http://localhost:$PORT${NC}"
echo -e "${GREEN}Press Ctrl+C to stop${NC}"
echo ""

export BATCH_API_URL
export S3_BUCKET_NAME

python app.py