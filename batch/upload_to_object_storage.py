import os
import shutil
from pathlib import Path
from datetime import datetime, timezone
import oci

# -----------------------------
# CONFIG
# -----------------------------
RAW_BASE_DIR = Path("data/raw")
UPLOADED_BASE_DIR = Path("data/uploaded")

BUCKET_NAME = "crypto-raw"
SUCCESS_FILENAME = "_SUCCESS"


# -----------------------------
# OCI CLIENT
# -----------------------------
def get_object_storage_client():
    config = oci.config.from_file()
    client = oci.object_storage.ObjectStorageClient(config)
    namespace = client.get_namespace().data
    return client, namespace


# -----------------------------
# FILE DISCOVERY
# -----------------------------
def discover_parquet_files():
    if not RAW_BASE_DIR.exists():
        return []

    return [
        p for p in RAW_BASE_DIR.rglob("*.parquet")
        if p.is_file()
    ]


def local_path_to_object_key(local_path: Path) -> str:
    # Preserve directory structure under data/raw
    return str(local_path.relative_to(RAW_BASE_DIR))


# -----------------------------
# UPLOAD + ARCHIVE
# -----------------------------
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


# -----------------------------
# DATE COMPLETENESS LOGIC
# -----------------------------
def is_date_closed(date_str: str) -> bool:
    """
    A date is closed ONLY if it is strictly before today (UTC).
    This prevents marking _SUCCESS while ingest is still running.
    """
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    today = datetime.now(timezone.utc).date()
    return date < today


def write_success_marker(client, namespace, object_prefix: str):
    """
    Writes crypto-raw/<prefix>/_SUCCESS once (idempotent).
    """
    success_key = f"{object_prefix}/{SUCCESS_FILENAME}"

    existing = client.list_objects(
        namespace,
        BUCKET_NAME,
        prefix=success_key
    ).data.objects

    if existing:
        return  # already written

    client.put_object(
        namespace_name=namespace,
        bucket_name=BUCKET_NAME,
        object_name=success_key,
        put_object_body=b"ok"
    )

    print(f"Wrote _SUCCESS marker: {success_key}")


def find_closed_date_dirs():
    """
    Finds (exchange, symbol, date) directories that:
    - Have no remaining parquet files locally
    - Represent a date that is closed (date < today UTC)
    """
    completed = set()

    if not RAW_BASE_DIR.exists():
        return completed

    for exchange_dir in RAW_BASE_DIR.iterdir():
        if not exchange_dir.is_dir():
            continue

        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue

            for date_dir in symbol_dir.iterdir():
                if not date_dir.is_dir():
                    continue

                date_str = date_dir.name

                # Skip if date is still open
                if not is_date_closed(date_str):
                    continue

                # Skip if any parquet still pending
                if any(date_dir.rglob("*.parquet")):
                    continue

                completed.add(
                    (exchange_dir.name, symbol_dir.name, date_str)
                )

    return completed


# -----------------------------
# MAIN
# -----------------------------
def main():
    client, namespace = get_object_storage_client()

    files = discover_parquet_files()

    if not files:
        print("No parquet files to upload.")
    else:
        print(f"Found {len(files)} parquet files to upload.")

    # Upload all available parquet files
    for path in files:
        object_key = local_path_to_object_key(path)
        try:
            print(f"Uploading {path} → {object_key}")
            upload_file(client, namespace, path, object_key)
            archive_file(path)
        except Exception as e:
            print(f"FAILED {path}: {e}")

    # Write _SUCCESS markers ONLY for closed dates
    completed_dirs = find_closed_date_dirs()

    for exchange, symbol, date in completed_dirs:
        object_prefix = f"{exchange}/{symbol}/{date}"
        write_success_marker(client, namespace, object_prefix)


if __name__ == "__main__":
    main()
