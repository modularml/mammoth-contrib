# mammoth-contrib

Example and support code for interacting with Mammoth

## Projects

### [batch-manager](./batch-manager/)

A lightweight web interface for managing Mammoth Batch API jobs with integrated S3 presigned URL generation.

**Features:**
- Web UI for creating, viewing, retrying, and canceling batch jobs
- Built-in S3 presigned URL generation
- AWS credentials management
- Configuration presets with import/export
- Real-time batch status monitoring
- Curl command generation for all operations

**Quick Start:**
```bash
cd batch-manager
./start.sh
```

Then open http://localhost:8080

### [batch](./batch/)

Python scripts for batch job management via command line.

**Scripts:**
- `generate-s3-presigned.py`: Generate S3 presigned URLs for batch I/O
- `submit-job.py`: Submit batch jobs via CLI
- `make-batch.py`: Create batch files and configurations

## Example API Usage

```bash
curl -X POST http://localhost:8000/v1/batches \
    -H "Content-Type: application/json" \
    -d '{
    "batch_id": "batch-example-001",
    "input_file_id": "https://your-bucket.s3.amazonaws.com/inputs/batch.tar.gz?...",
    "output_file_id": "https://your-bucket.s3.amazonaws.com/outputs/batch-output.tar.gz?...",
    "endpoint": "/v1/chat/completions",
    "completion_window": "24h",
    "metadata": {
        "model": "OpenGVLab/InternVL3-38B-Instruct",
        "output_file_id": ""
    }
}'
```