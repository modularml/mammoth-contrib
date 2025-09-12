"""
Mammoth Batch Manager - Web UI for managing batch jobs
"""

import os
import sys
import json
import datetime
from typing import Optional, Dict, Any, List

# Add the batch directory to Python path to import the existing script
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'batch'))

import boto3
import httpx
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import the existing S3 presigned URL generation function (required)
from generate_s3_presigned import generate_s3_presigned_urls as generate_s3_urls


# Configuration
BATCH_API_URL = os.getenv("BATCH_API_URL", "http://localhost:8000")
DEFAULT_S3_BUCKET = os.getenv("S3_BUCKET_NAME", "modular-batch-api-batches")
DEFAULT_EXPIRES_IN = 604800  # 7 days in seconds


# Simple validation functions
def validate_batch_request(data: dict):
    """Validate batch creation request"""
    required_fields = ["batch_id", "input_file_id", "output_file_id", "endpoint", "completion_window"]
    for field in required_fields:
        if field not in data or not data[field]:
            raise ValueError(f"Field '{field}' is required")
    
    valid_windows = ["6h", "12h", "24h", "7d"]
    if data["completion_window"] not in valid_windows:
        raise ValueError(f"completion_window must be one of {valid_windows}")
    
    if len(data["batch_id"]) < 3:
        raise ValueError("batch_id must be at least 3 characters long")
    
    return data


# Global HTTP client
http_client: Optional[httpx.AsyncClient] = None

app = FastAPI(
    title="Mammoth Batch Manager",
    description="Web UI for managing Mammoth batch jobs",
    version="1.0.0"
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.on_event("startup")
async def startup_event():
    global http_client
    http_client = httpx.AsyncClient(timeout=30.0)

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def generate_s3_presigned_urls(
    bucket: str,
    input_key: str,
    output_key: Optional[str] = None,
    credentials: Optional[Dict[str, str]] = None,
    expires_in: int = DEFAULT_EXPIRES_IN
) -> Dict[str, Any]:
    """Generate S3 presigned URLs for batch job input and output files using existing script."""
    
    # Extract credentials for the existing function signature
    aws_access_key_id = credentials.get("aws_access_key_id") if credentials else None
    aws_secret_access_key = credentials.get("aws_secret_access_key") if credentials else None
    aws_session_token = credentials.get("aws_session_token") if credentials else None
    
    # Use the existing generate-s3-presigned.py script
    result = generate_s3_urls(
        bucket=bucket,
        input_key=input_key,
        output_key=output_key,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        expires_in=expires_in
    )
    
    # Add bucket to result for compatibility with our API response format
    result["bucket"] = bucket
    return result


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main UI."""
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())


@app.get("/api/health")
async def health_check():
    """Check health of batch API and S3 access."""
    
    # Check batch API
    batch_api_status = "unknown"
    try:
        response = await http_client.get(f"{BATCH_API_URL}/health")
        batch_api_status = "healthy" if response.status_code == 200 else "unhealthy"
    except Exception as e:
        batch_api_status = f"error: {str(e)}"
    
    # Check S3 access
    s3_access = False
    aws_region = "unknown"
    try:
        s3_client = boto3.client("s3")
        s3_client.head_bucket(Bucket=DEFAULT_S3_BUCKET)
        s3_access = True
        aws_region = s3_client.meta.region_name
    except:
        pass
    
    return {
        "batch_api_status": batch_api_status,
        "batch_api_url": BATCH_API_URL,
        "s3_access": s3_access,
        "aws_region": aws_region,
        "timestamp": datetime.datetime.now().isoformat()
    }


@app.post("/api/generate-presigned-urls")
async def generate_presigned_urls(request: Request):
    """Generate S3 presigned URLs for batch input/output."""
    try:
        data = await request.json()
        result = generate_s3_presigned_urls(
            bucket=data.get("bucket", DEFAULT_S3_BUCKET),
            input_key=data["input_key"],
            output_key=data.get("output_key"),
            credentials=data.get("credentials"),
            expires_in=data.get("expires_in", DEFAULT_EXPIRES_IN)
        )
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batches")
async def list_batches(limit: int = Query(100, ge=1, le=500), after: Optional[str] = None):
    """List all batches from the batch API."""
    try:
        params = {"limit": limit}
        if after:
            params["after"] = after
        
        response = await http_client.get(
            f"{BATCH_API_URL}/v1/batches",
            params=params
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/batches/{batch_id}")
async def get_batch(batch_id: str):
    """Get a specific batch by ID."""
    try:
        response = await http_client.get(f"{BATCH_API_URL}/v1/batches/{batch_id}")
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/batches")
async def create_batch(request: Request):
    """Create a new batch."""
    try:
        data = await request.json()
        validate_batch_request(data)
        
        response = await http_client.post(
            f"{BATCH_API_URL}/v1/batches",
            json=data
        )
        response.raise_for_status()
        return response.json()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/batches/{batch_id}/cancel")
async def cancel_batch(batch_id: str):
    """Cancel a batch."""
    try:
        response = await http_client.post(f"{BATCH_API_URL}/v1/batches/{batch_id}/cancel")
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/batches/{batch_id}/retry")
async def retry_batch(batch_id: str):
    """Retry a batch."""
    try:
        response = await http_client.post(f"{BATCH_API_URL}/v1/batches/{batch_id}/retry")
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/validate-s3-key/{key:path}")
async def validate_s3_key(key: str, bucket: str = Query(DEFAULT_S3_BUCKET)):
    """Validate if an S3 key exists."""
    try:
        s3_client = boto3.client("s3")
        s3_client.head_object(Bucket=bucket, Key=key)
        return {"exists": True, "bucket": bucket, "key": key}
    except:
        return {"exists": False, "bucket": bucket, "key": key}


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info"
    )