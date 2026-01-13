from http import client
import os 
import shutil
from pathlib import Path
from asyncio.tools import build_async_tree
from numpy import object_
import oci 

RAW_BASE_DIR = Path("data/raw")
UPLOADED_BASE_DIR = Path("data/uploaded")

BUCKET_NAME = "crypto_raw"

def get_object_storage_client():
    config = oci.config.from_file()  # Assumes default config file and profile
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data
    return client, namespace

def discover_parquet_files():
    if not RAW_BASE_DIR.exists():
        return []
    
    return [
        path 
        for path in RAW_BASE_DIR.rglob("*.parquet")
        if path.is_file()
    ]

def local_path_to_object_key(local_path : Path) -> str:
    return str(local_path.relative_to(RAW_BASE_DIR))

def upload_file(client, namespace, local_path: Path, object_key: str):
    with local_path.open("rb") as f:
        client.put_object(
            namespace_name=namespace,
            bucket_name=BUCKET_NAME,
            object_name=object_key,
            put_object_body=f
        )

def archive_file(local_path: Path):
    destination = UPLOADED_BASE_DIR / local_path.relative_to(RAW_BASE_DIR)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(local_path), str(destination))

def main():
    client, namespace = get_object_storage_client()

    files = discover_parquet_files()
    if not files:
        print("No files to upload.")
        return
    
    print(f"Found {len(files)} files to upload.")

    for path in files:
        object_key = local_path_to_object_key(path)

        try:
            print(f"Uploading {path} -> {object_key}...")
            upload_file(client, namespace, path, object_key)
            archive_file(path)
            print(f"Uploaded and archived {path}.")
        
        except Exception as e:
            print(f"FAILED {path}: {e}")

if __name__ == "__main__":
    main()