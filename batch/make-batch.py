import argparse
import json
import pathlib
import tarfile
import uuid


def make_request(prompt: str, rel_path: str):
    """Make an OpenAI compatible request"""
    return {
        "custom_id": uuid.uuid4().hex,
        "method":"POST",
        "url":"/v1/chat/completions",
        "body":{
            "model":"OpenGVLab/InternVL3-38B-Instruct",
            "messages":[
                {
                    "role":"user",
                    "content":[
                         {"type":"text", "text": prompt},
                         {"type":"image_url", "image_url": {"url": f"file:{rel_path}"}},
                     ],
                }
            ],
            "max_tokens":100,
        }
    }


def write_batch(prompt: str, suffix: str, batch: list[pathlib.Path]) -> pathlib.Path:
    """Write a batch file with all files in batch & a request file"""
    batch_path = pathlib.Path(f'batch-{suffix}.tar.gz')
    with tarfile.open(batch_path, "w:gz") as tf:
        # jobs_path is a jsonl-formatted file with a line/request per image
        jobs_path = "jobs-{suffix}.jsonl"
        with open(jobs_path, 'w') as f:
            for p in batch:
                rel_path = f'files/{p.name}'
                tf.add(p, arcname=rel_path)
                payload = json.dumps(make_request(prompt, rel_path))
                f.write(payload + '\n')
        tf.add(jobs_path, "jobs.jsonl")
    return batch_path


def write_batches(prompt: str, images_dir: pathlib.Path, batch_size: int):
    """Write batch files applying a prompt to all files in images_dir."""
    # partition into batches.
    for i, b in enumerate(iter_batches(images_dir, batch_size)):
        # make sure each batch has a unique name.
        suffix = f"{i:05d}"
        # write a file for each batch.
        bp = write_batch(prompt, suffix, b)
        print(f"written batch: {bp}")


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", type=str, required=True, help="Prompt to LLM")
    parser.add_argument("--images_dir", type=str, required=True, help="Path to directory containing images")
    parser.add_argument("--batch_size", type=int, required=True, help="How many images to include in a batch file")
    args = parser.parse_args()
    write_batches(args.prompt, pathlib.Path(args.images_dir), args.batch_size)


if __name__ == '__main__':
    main()
