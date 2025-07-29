"""Example script generate S3 signed urls for Mammoth BatchJob submission"""

import argparse
import os
import datetime

import boto3
import requests

def main(bucket, input_key, host):
    name = input_key.split('/')[-1]
    ts = datetime.datetime.now().isoformat()

    output_key = f"outputs/{name}/{ts}.tar.gz"

    s3_client = boto3.client("s3")

    # create a (readable) pre-signed url so Mammoth can read the batch job input
    input_file = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={"Bucket": bucket, "Key": input_key},
        ExpiresIn=604800,
    )

    # create a (writeable) pre-signed url so Mammoth can write batch job output
    output_file = s3_client.generate_presigned_url(
        ClientMethod='put_object',
        Params={"Bucket": bucket, "Key": output_key},
        ExpiresIn=604800,
    )

    # Submit the job to the Mammoth Batch API server
    payload = {
        "input_file_id": input_file,
        "output_file_id": output_file,
        "endpoint": "/v1/chat/completions",
        "completion_window": "24h",
        "metadata": {
          "model": "OpenGVLab/InternVL3-38B-Instruct",
          "output_file_id": output_file,
        },
    }
    print(requests.post(f"{host}/v1/batches", json=payload))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, type=str, help="S3 bucket with batch job input")
    parser.add_argument("--key", required=True, type=str, help="S3 key for batch job input")
    parser.add_argument("--host", required=True, type=str, help="Scheme, host (and port) to submit job et http://localhost:8000")
    args = parser.parse_args()
    main(args.bucket, args.key, args.host)
