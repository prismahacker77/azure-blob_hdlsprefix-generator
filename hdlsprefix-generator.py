
# adls_hash_prefix_uploader.py
import os
import sys
import hashlib
import datetime as dt
from pathlib import Path
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileSystemClient,
    DataLakeDirectoryClient,
)

# --------------------------
# Configuration (env-first)
# --------------------------
ACCOUNT_NAME      = os.getenv("AZURE_STORAGE_ACCOUNT", "mystorageacct")          # HNS-enabled account name
FILE_SYSTEM       = os.getenv("AZURE_STORAGE_FILESYSTEM", "datalake")            # ADLS Gen2 filesystem (a.k.a. container)
ZONE              = os.getenv("DATA_LAKE_ZONE", "raw")                           # raw | curated | trusted
DOMAIN            = os.getenv("DATA_LAKE_DOMAIN", "sales")                       # e.g., sales, inventory, marketing
SOURCE_DIR        = os.getenv("SOURCE_DIR", "./sample-data")                     # local folder with files to upload
DATE_ISO          = os.getenv("PARTITION_DATE", dt.date.today().isoformat())     # YYYY-MM-DD
PREFIX_LEN        = int(os.getenv("HASH_PREFIX_LEN", "4"))                       # 3–6 recommended
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")                 # optional
ACCOUNT_URL       = f"https://{ACCOUNT_NAME}.dfs.core.windows.net"

# --------------------------
# Helpers
# --------------------------
def get_service_client() -> DataLakeServiceClient:
    """
    Creates a DataLakeServiceClient using either:
      - Connection string (if provided), or
      - DefaultAzureCredential against dfs endpoint (recommended for dev/MI).
    """
    if CONNECTION_STRING:
        return DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    cred = DefaultAzureCredential(exclude_interactive_browser_credential=False)
    return DataLakeServiceClient(account_url=ACCOUNT_URL, credential=cred)

def ensure_filesystem(svc: DataLakeServiceClient, fs_name: str) -> FileSystemClient:
    try:
        fs = svc.create_file_system(file_system=fs_name)
        return fs
    except ResourceExistsError:
        return svc.get_file_system_client(fs_name)

def ensure_directory(fs: FileSystemClient, path: str) -> DataLakeDirectoryClient:
    """
    Creates (nested) directory if it doesn't already exist.
    """
    try:
        return fs.create_directory(path)
    except ResourceExistsError:
        return fs.get_directory_client(path)

def sha_prefix(name: str, length: int = 4) -> str:
    """
    Returns a short, hex hash prefix of the provided name.
    """
    h = hashlib.sha256(name.encode("utf-8")).hexdigest()
    return h[:max(1, length)]

def upload_file(fs: FileSystemClient, local_path: Path, target_dir: str, prefix_len: int) -> str:
    """
    Uploads a single file with a hash prefix in the filename.
    Returns the ADLS path that was created.
    """
    file_name = local_path.name
    prefix = sha_prefix(file_name, prefix_len)
    target_file_name = f"{prefix}-{file_name}"
    adls_path = f"{target_dir}/{target_file_name}".replace("//", "/")

    # Create file client directly via directory client to ensure path exists
    dir_client = ensure_directory(fs, target_dir)
    file_client = dir_client.create_file(target_file_name)

    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    # (Optional) flush is handled internally by upload_data
    return adls_path

def build_partition_path(zone: str, domain: str, date_iso: str) -> str:
    d = dt.datetime.fromisoformat(date_iso).date()
    return f"{zone}/{domain}/year={d.year}/month={d.month:02d}/day={d.day:02d}"

def main():
    # Validate inputs
    src_dir = Path(SOURCE_DIR)
    if not src_dir.exists() or not src_dir.is_dir():
        print(f"[!] SOURCE_DIR not found: {src_dir.resolve()}")
        sys.exit(1)

    print(f"[i] Connecting to account: {ACCOUNT_NAME}")
    svc = get_service_client()

    print(f"[i] Ensuring filesystem: {FILE_SYSTEM}")
    fs = ensure_filesystem(svc, FILE_SYSTEM)

    target_dir = build_partition_path(ZONE, DOMAIN, DATE_ISO)
    print(f"[i] Target ADLS directory: {target_dir}")

    # Ensure target directory exists (creates nested tree)
    ensure_directory(fs, target_dir)

    # Upload all files (non-recursive by default)
    uploaded = []
    for p in sorted(src_dir.iterdir()):
        if p.is_file():
            adls_path = upload_file(fs, p, target_dir, PREFIX_LEN)
            uploaded.append((p.name, adls_path))
            print(f"[✓] {p.name}  →  {adls_path}")

    if not uploaded:
        print("[i] No files found to upload.")
        return

    print("\nSummary:")
    for (src, dst) in uploaded:
        print(f"  - {src} → {dst}")

if __name__ == "__main__":
    main()
