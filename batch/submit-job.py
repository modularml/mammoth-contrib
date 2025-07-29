"""Example script to submit batch jobs to SFC API using presigned URLs"""

import argparse

import requests


def submit_batch_job(input_file_uri, output_file_uri, bearer_token, host="https://api.sfcompute.com", model="OpenGVLab/InternVL3-38B-Instruct", endpoint="/v1/chat/completions", completion_window="24h", store="s3"):
    """
    Submit a batch job to the SFC API.
    
    Args:
        input_file_uri (str): Presigned URL for batch job input file
        output_file_uri (str): Presigned URL for batch job output file
        bearer_token (str): SFC API Bearer token for authentication
        host (str): SFC API host (default: https://api.sfcompute.com)
        model (str): Model to use for batch processing
        endpoint (str): API endpoint for the batch job
        completion_window (str): Time window for completion
        store (str): Storage backend type
    
    Returns:
        dict: Response from the SFC API containing batch ID and status
        
    Raises:
        requests.exceptions.HTTPError: If the HTTP request fails
        requests.exceptions.RequestException: If the request fails for other reasons
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}"
    }
    
    payload = {
        "input_file_uri": input_file_uri,
        "output_file_uri": output_file_uri,
        "endpoint": endpoint,
        "completion_window": completion_window,
        "store": store,
        "model_id": model,
    }

    response = requests.post(f"{host}/v1/inference/batches", headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()[0]


def main(input_file_uri, output_file_uri, bearer_token, host, model):
    try:
        result = submit_batch_job(input_file_uri, output_file_uri, bearer_token, host, model)
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
    parser.add_argument("--input-file-uri", required=True, type=str, help="Presigned S3 URL for batch job input file")
    parser.add_argument("--output-file-uri", required=True, type=str, help="Presigned S3 URL for batch job output file")
    parser.add_argument("--bearer-token", required=True, type=str, help="SFC API Bearer token for authentication")
    parser.add_argument("--host", default="https://api.sfcompute.com", type=str, help="SFC API host (default: https://api.sfcompute.com)")
    parser.add_argument("--model", default="OpenGVLab/InternVL3-38B-Instruct", type=str, help="Model to use for batch processing (default: OpenGVLab/InternVL3-38B-Instruct)")
    args = parser.parse_args()
    
    main(args.input_file_uri, args.output_file_uri, args.bearer_token, args.host, args.model)
