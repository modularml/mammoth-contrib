import argparse
import json
from typing import Optional
import pathlib
import tarfile
import uuid


def make_request(prompt: str, rel_path: str, model: str = "OpenGVLab/InternVL3-38B-Instruct", max_tokens: int = 100, custom_id: Optional[str] = None):
    """
    Make an OpenAI compatible request for batch processing.
    
    Args:
        prompt (str): Text prompt for the request
        rel_path (str): Relative path to the image file within the batch
        model (str): Model to use for the request
        max_tokens (int): Maximum tokens in the response
        custom_id (str): Custom ID for the request (generates UUID if None)
    
    Returns:
        dict: OpenAI compatible batch request format
    """
    if custom_id is None:
        custom_id = uuid.uuid4().hex
        
    return {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                         {"type": "text", "text": prompt},
                         {"type": "image_url", "image_url": {"url": f"file:{rel_path}"}},
                     ],
                }
            ],
            "max_tokens": max_tokens,
        }
    }


def write_batch(prompt: str, suffix: str, batch: list[pathlib.Path], model: str = "OpenGVLab/InternVL3-38B-Instruct", max_tokens: int = 100, output_dir: Optional[pathlib.Path] = None) -> pathlib.Path:
    """
    Write a batch file with all files in batch & a request file.
    
    Args:
        prompt (str): Text prompt for all requests in the batch
        suffix (str): Suffix for the batch filename
        batch (list[pathlib.Path]): List of image file paths to include
        model (str): Model to use for all requests
        max_tokens (int): Maximum tokens for responses
        output_dir (pathlib.Path): Directory to write the batch file (default: current dir)
    
    Returns:
        pathlib.Path: Path to the created batch tar.gz file
    """
    if output_dir is None:
        output_dir = pathlib.Path(".")
    
    batch_path = output_dir / f'batch-{suffix}.tar.gz'
    jobs_filename = f"jobs-{suffix}.jsonl"
    
    with tarfile.open(batch_path, "w:gz") as tf:
        # jobs_path is a jsonl-formatted file with a line/request per image
        with open(jobs_filename, 'w') as f:
            for p in batch:
                rel_path = f'files/{p.name}'
                tf.add(p, arcname=rel_path)
                payload = json.dumps(make_request(prompt, rel_path, model, max_tokens))
                f.write(payload + '\n')
        tf.add(jobs_filename, "jobs.jsonl")
    
    # clean up the temporary jobs file
    pathlib.Path(jobs_filename).unlink(missing_ok=True)
    return batch_path


def write_batches(prompt: str, images_dir: pathlib.Path, batch_size: int, model: str = "OpenGVLab/InternVL3-38B-Instruct", max_tokens: int = 100, output_dir: Optional[pathlib.Path] = None, verbose: bool = True) -> list[pathlib.Path]:
    """
    Write batch files applying a prompt to all files in images_dir.
    
    Args:
        prompt (str): Text prompt for all requests
        images_dir (pathlib.Path): Directory containing image files
        batch_size (int): Number of images per batch file
        model (str): Model to use for all requests
        max_tokens (int): Maximum tokens for responses
        output_dir (pathlib.Path): Directory to write batch files (default: current dir)
        verbose (bool): Whether to print progress messages
    
    Returns:
        list[pathlib.Path]: List of paths to created batch files
    """
    batch_files = []
    
    # partition into batches.
    for i, b in enumerate(iter_batches(images_dir, batch_size)):
        # make sure each batch has a unique name.
        suffix = f"{i:05d}"
        # write a file for each batch.
        bp = write_batch(prompt, suffix, b, model, max_tokens, output_dir)
        batch_files.append(bp)
        if verbose:
            print(f"written batch: {bp}")
    
    return batch_files


def iter_batches(images_dir: pathlib.Path, batch_size: int):
    """Traverse a directory of files and yield batches having at most `batch_size` entries."""
    batch: list[pathlib.Path] = []
    for p in images_dir.iterdir():
        if not p.is_file():
            continue
        batch.append(p)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if len(batch) >= 0:
        yield batch


def main():
    """CLI entry point for the batch creation script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", type=str, required=True, help="Prompt to LLM")
    parser.add_argument("--images_dir", type=str, required=True, help="Path to directory containing images")
    parser.add_argument("--batch_size", type=int, required=True, help="How many images to include in a batch file")
    parser.add_argument("--model", type=str, default="OpenGVLab/InternVL3-38B-Instruct", help="Model to use for batch processing")
    parser.add_argument("--max_tokens", type=int, default=100, help="Maximum tokens for responses")
    parser.add_argument("--output_dir", type=str, help="Directory to write batch files (default: current directory)")
    args = parser.parse_args()
    
    output_dir = pathlib.Path(args.output_dir) if args.output_dir else None
    write_batches(args.prompt, pathlib.Path(args.images_dir), args.batch_size, args.model, args.max_tokens, output_dir)


if __name__ == '__main__':
    main()
