"""Example script generate S3 signed urls for SFC Batch API batch submission"""

import argparse
import datetime

import boto3
import requests

def main(bucket, input_key, bearer_token, host, input_price, output_price, model, aws_access_key_id, aws_secret_access_key, aws_session_token, expires_in):
    name = input_key.split('/')[-1]
    ts = datetime.datetime.now().isoformat()

    output_key = f"outputs/{name}/{ts}.tar.gz"

    # create S3 client with custom credentials if provided
    if aws_access_key_id and aws_secret_access_key:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
    else:
        s3_client = boto3.client("s3")

    # create a (readable) pre-signed url so SFC can read the batch job input
    input_file_uri = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={"Bucket": bucket, "Key": input_key},
        ExpiresIn=expires_in,
    )

    # create a (writeable) pre-signed url so SFC can write batch job output
    output_file_uri = s3_client.generate_presigned_url(
        ClientMethod='put_object',
        Params={"Bucket": bucket, "Key": output_key},
        ExpiresIn=expires_in,
    )

    # Submit the job to the SFC Batch API server
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }
    
    payload = {
        "input_file_uri": input_file_uri,
        "output_file_uri": output_file_uri,
        "endpoint": "/v1/chat/completions",
        "completion_window": "24h",
        "store": "s3",
        "price": {
            "cents_per_million_input_tokens": input_price,
            "cents_per_million_output_tokens": output_price
        },
        "metadata": {
            "model": model,
        },
    }
    
    try:
        response = requests.post(f"{host}/v1/inference/batches", headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        print("Batch job submitted successfully:")
        print(f"Batch ID: {result.get('id', 'N/A')}")
        print(f"Status: {result.get('status', 'N/A')}")
        return result
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        if hasattr(e.response, 'text'):
            print(f"Response: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, type=str, help="S3 bucket with batch job input")
    parser.add_argument("--key", required=True, type=str, help="S3 key for batch job input")
    parser.add_argument("--bearer-token", required=True, type=str, help="SFC API Bearer token for authentication")
    parser.add_argument("--host", default="https://api.sfcompute.com", type=str, help="SFC API host (default: https://api.sfcompute.com)")
    parser.add_argument("--input-price", default=100, type=int, help="Price in cents per million input tokens (default: 100)")
    parser.add_argument("--output-price", default=100, type=int, help="Price in cents per million output tokens (default: 100)")
    parser.add_argument("--model", default="OpenGVLab/InternVL3-38B-Instruct", type=str, help="Model to use for batch processing (default: OpenGVLab/InternVL3-38B-Instruct)")
    parser.add_argument("--aws-access-key-id", type=str, help="AWS Access Key ID (if not provided, uses default credentials)")
    parser.add_argument("--aws-secret-access-key", type=str, help="AWS Secret Access Key (required if access key is provided)")
    parser.add_argument("--aws-session-token", type=str, help="AWS Session Token (optional, for temporary credentials)")
    parser.add_argument("--expires-in", default=604800, type=int, help="S3 URL expiry time in seconds (default: 604800 = 7 days)")
    args = parser.parse_args()
    
    # validate AWS credentials
    if args.aws_access_key_id and not args.aws_secret_access_key:
        parser.error("--aws-secret-access-key is required when --aws-access-key-id is provided")
    
    main(args.bucket, args.key, args.bearer_token, args.host, args.input_price, args.output_price, args.model, 
         args.aws_access_key_id, args.aws_secret_access_key, args.aws_session_token, args.expires_in)
