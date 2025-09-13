"""Script to generate S3 presigned URLs for batch job input and output"""

import argparse
import datetime

import boto3


def generate_s3_presigned_urls(bucket, input_key, output_key=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, expires_in=604800):
    """
    Generate S3 presigned URLs for batch job input and output files.
    
    Args:
        bucket (str): S3 bucket name
        input_key (str): S3 key for input file
        output_key (str): S3 key for output file (auto-generated if None)
        aws_access_key_id (str): AWS Access Key ID (uses default credentials if None)
        aws_secret_access_key (str): AWS Secret Access Key
        aws_session_token (str): AWS Session Token (optional for temporary credentials)
        expires_in (int): URL expiry time in seconds
    
    Returns:
        dict: Dictionary containing presigned URLs and metadata:
            - input_url: Presigned GET URL for input file
            - output_url: Presigned PUT URL for output file
            - input_key: S3 key for input file
            - output_key: S3 key for output file (generated or provided)
            - expires_in: Expiry time in seconds
    
    Raises:
        boto3.exceptions.Boto3Error: If S3 client creation or URL generation fails
    """
    # create output key based on input key if not provided
    if not output_key:
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

    # create a (readable) pre-signed url for batch job input
    input_file_uri = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={"Bucket": bucket, "Key": input_key},
        ExpiresIn=expires_in,
    )

    # create a (writeable) pre-signed url for batch job output
    output_file_uri = s3_client.generate_presigned_url(
        ClientMethod='put_object',
        Params={"Bucket": bucket, "Key": output_key},
        ExpiresIn=expires_in,
    )

    return {
        "input_url": input_file_uri,
        "output_url": output_file_uri,
        "input_key": input_key,
        "output_key": output_key,
        "expires_in": expires_in
    }


def main(bucket, input_key, output_key=None, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, expires_in=604800):
    """CLI wrapper function that calls generate_presigned_urls and prints results"""
    try:
        result = generate_s3_presigned_urls(bucket, input_key, output_key, aws_access_key_id, aws_secret_access_key, aws_session_token, expires_in)
        
        # Print the presigned URLs
        print("S3 Presigned URLs generated successfully:")
        print(f"Input URL (GET): {result['input_url']}")
        print(f"Output URL (PUT): {result['output_url']}")
        print(f"Input Key: {result['input_key']}")
        print(f"Output Key: {result['output_key']}")
        print(f"Expires in: {result['expires_in']} seconds")
        
        return result
    except Exception as e:
        print(f"Error generating presigned URLs: {e}")
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, type=str, help="S3 bucket name")
    parser.add_argument("--input-key", required=True, type=str, help="S3 key for input file")
    parser.add_argument("--output-key", type=str, help="S3 key for output file (default: generated based on input key)")
    parser.add_argument("--aws-access-key-id", type=str, help="AWS Access Key ID (if not provided, uses default credentials)")
    parser.add_argument("--aws-secret-access-key", type=str, help="AWS Secret Access Key (required if access key is provided)")
    parser.add_argument("--aws-session-token", type=str, help="AWS Session Token (optional, for temporary credentials)")
    parser.add_argument("--expires-in", default=604800, type=int, help="S3 URL expiry time in seconds (default: 604800 = 7 days)")
    args = parser.parse_args()
    
    # validate AWS credentials
    if args.aws_access_key_id and not args.aws_secret_access_key:
        parser.error("--aws-secret-access-key is required when --aws-access-key-id is provided")
    
    main(args.bucket, args.input_key, args.output_key, 
         args.aws_access_key_id, args.aws_secret_access_key, args.aws_session_token, args.expires_in)