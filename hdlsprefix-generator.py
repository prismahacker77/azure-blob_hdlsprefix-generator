#!/usr/bin/env python3
"""
Azure Blob Storage Hash-Prefix Generator

Interactive cross-platform script for Azure Blob Storage that:
- Creates or reuses resource groups, storage accounts, and containers
- Configures HNS/ADLS Gen2, SKU, redundancy, TLS settings
- Uploads files with hash prefixes (e.g., aa/bb/yourfile.parquet) to improve
  partition distribution and performance

Why hash prefixes? Azure partitions by a key that includes the blob name.
Sequential names can concentrate traffic; adding an early hash prefix spreads
load and improves ingest/list performance.
"""

import argparse
import hashlib
import os
import re
import sys
import time
import threading
import itertools
import tempfile
import random
import string
import warnings
from pathlib import Path
from typing import Optional, List, Tuple

# Suppress urllib3 LibreSSL warning
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import (
    StorageAccountCreateParameters,
    Sku,
    Kind,
)
from azure.storage.blob import BlobServiceClient


# ==============================================================================
# ANSI Colors
# ==============================================================================
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    AZURE_MID = "\033[38;5;67m"
    AZURE_LIGHT = "\033[38;5;110m"
    AZURE_ACCENT = "\033[38;5;73m"
    SUCCESS = "\033[38;5;108m"
    ERROR = "\033[38;5;131m"
    INFO = "\033[38;5;145m"
    WARN = "\033[38;5;179m"

    @classmethod
    def disable(cls):
        for attr in dir(cls):
            if attr.isupper() and not attr.startswith('_'):
                setattr(cls, attr, '')


if not sys.stdout.isatty():
    Colors.disable()


# ==============================================================================
# Spinner
# ==============================================================================
class Spinner:
    FRAMES = ['|', '/', '-', '\\']

    def __init__(self, message: str = "Working"):
        self.message = message
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _animate(self):
        frames = itertools.cycle(self.FRAMES)
        while not self._stop_event.is_set():
            frame = next(frames)
            sys.stdout.write(
                f"\r{Colors.AZURE_ACCENT}{frame}{Colors.RESET} "
                f"{Colors.AZURE_LIGHT}{self.message}{Colors.RESET}"
            )
            sys.stdout.flush()
            time.sleep(0.1)

    def start(self):
        if sys.stdout.isatty():
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._animate, daemon=True)
            self._thread.start()

    def stop(self, success: bool = True, message: str = ""):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=0.5)
        if sys.stdout.isatty():
            sys.stdout.write('\r' + ' ' * 80 + '\r')
            sys.stdout.flush()
        if message:
            icon = f"{Colors.SUCCESS}+" if success else f"{Colors.ERROR}!"
            print(f"  {icon}{Colors.RESET} {Colors.INFO}{message}{Colors.RESET}")


# ==============================================================================
# Configuration Options
# ==============================================================================
SKU_OPTIONS = {
    "1": ("Standard_LRS", "Standard Locally Redundant Storage"),
    "2": ("Standard_GRS", "Standard Geo-Redundant Storage"),
    "3": ("Standard_RAGRS", "Standard Read-Access Geo-Redundant Storage"),
    "4": ("Standard_ZRS", "Standard Zone-Redundant Storage"),
    "5": ("Standard_GZRS", "Standard Geo-Zone-Redundant Storage"),
    "6": ("Standard_RAGZRS", "Standard Read-Access Geo-Zone-Redundant Storage"),
    "7": ("Premium_LRS", "Premium Locally Redundant Storage"),
    "8": ("Premium_ZRS", "Premium Zone-Redundant Storage"),
}


# ==============================================================================
# Prompts
# ==============================================================================
def prompt(label: str, default: str = "", required: bool = True) -> str:
    """Prompt user for input with optional default."""
    if default:
        display = f"  {Colors.AZURE_MID}{label}{Colors.RESET} [{Colors.DIM}{default}{Colors.RESET}]: "
    else:
        display = f"  {Colors.AZURE_MID}{label}{Colors.RESET}: "

    while True:
        value = input(display).strip()
        if not value and default:
            return default
        if value:
            return value
        if not required:
            return ""
        print(f"  {Colors.ERROR}! Required{Colors.RESET}")


def prompt_yes_no(label: str, default: bool = True) -> bool:
    """Prompt for yes/no with default."""
    default_str = "Y/n" if default else "y/N"
    display = f"  {Colors.AZURE_MID}{label}{Colors.RESET} [{Colors.DIM}{default_str}{Colors.RESET}]: "

    while True:
        value = input(display).strip().lower()
        if not value:
            return default
        if value in ('y', 'yes'):
            return True
        if value in ('n', 'no'):
            return False
        print(f"  {Colors.ERROR}! Enter 'y' or 'n'{Colors.RESET}")


def prompt_choice(label: str, options: dict, default: str = "1") -> str:
    """Prompt for choice from numbered options."""
    print(f"\n  {Colors.AZURE_MID}{label}{Colors.RESET}")
    for key, (value, desc) in options.items():
        print(f"    {Colors.DIM}{key}.{Colors.RESET} {value} - {Colors.DIM}{desc}{Colors.RESET}")

    while True:
        choice = prompt("Choice", default)
        if choice in options:
            return options[choice][0]
        print(f"  {Colors.ERROR}! Enter a number from 1-{len(options)}{Colors.RESET}")


def prompt_int(label: str, default: int, min_val: int = 1, max_val: int = 100) -> int:
    """Prompt for an integer within a range."""
    while True:
        value = prompt(label, str(default))
        try:
            int_val = int(value)
            if min_val <= int_val <= max_val:
                return int_val
            print(f"  {Colors.ERROR}! Enter a number between {min_val} and {max_val}{Colors.RESET}")
        except ValueError:
            print(f"  {Colors.ERROR}! Enter a valid number{Colors.RESET}")


def prompt_create_or_use(resource_type: str) -> str:
    """Prompt for create new or use existing."""
    print(f"\n  {Colors.AZURE_MID}{resource_type}{Colors.RESET}")
    print(f"    {Colors.DIM}1.{Colors.RESET} Create new")
    print(f"    {Colors.DIM}2.{Colors.RESET} Use existing")

    while True:
        choice = prompt("Choice", "2")
        if choice == "1":
            return "create"
        if choice == "2":
            return "existing"
        print(f"  {Colors.ERROR}! Enter 1 or 2{Colors.RESET}")


def prompt_storage_account_name(label: str, default: str = "") -> str:
    """Prompt for storage account name with validation."""
    while True:
        value = prompt(label, default)
        # Rules: 3-24 chars, lowercase alphanumeric only
        if re.match(r'^[a-z0-9]{3,24}$', value):
            return value
        print(f"  {Colors.ERROR}! Must be 3-24 lowercase alphanumeric characters{Colors.RESET}")


def prompt_resource_group_name(label: str, default: str = "") -> str:
    """Prompt for resource group name with validation."""
    while True:
        value = prompt(label, default)
        # Rules: 1-90 chars, alphanumeric, underscores, hyphens, periods
        if re.match(r'^[a-zA-Z0-9._-]{1,90}$', value):
            return value
        print(f"  {Colors.ERROR}! Must be 1-90 alphanumeric, underscore, hyphen, or period chars{Colors.RESET}")


def prompt_container_name(label: str, default: str = "") -> str:
    """Prompt for container name with validation."""
    while True:
        value = prompt(label, default)
        # Rules: 3-63 chars, lowercase, numbers, hyphens (no consecutive hyphens)
        if re.match(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$', value) and len(value) >= 3 and len(value) <= 63:
            if '--' not in value:
                return value
        print(f"  {Colors.ERROR}! Must be 3-63 lowercase alphanumeric with hyphens (no consecutive hyphens){Colors.RESET}")


# ==============================================================================
# Hash Prefix Generator
# ==============================================================================
def generate_hash_prefix(filename: str, depth: int = 2, chars_per_level: int = 2) -> str:
    """
    Generate a hash prefix for a filename to improve blob distribution.

    Args:
        filename: The original filename
        depth: Number of directory levels (e.g., 2 = aa/bb/)
        chars_per_level: Characters per directory level (e.g., 2 = aa)

    Returns:
        Hash prefix path (e.g., "a3/f7/" for depth=2, chars_per_level=2)
    """
    # Use MD5 hash of filename for consistent distribution
    hash_hex = hashlib.md5(filename.encode('utf-8')).hexdigest()

    prefix_parts = []
    for i in range(depth):
        start = i * chars_per_level
        end = start + chars_per_level
        prefix_parts.append(hash_hex[start:end])

    return '/'.join(prefix_parts) + '/'


def get_prefixed_blob_name(filename: str, depth: int = 2, chars_per_level: int = 2) -> str:
    """Get the full blob name with hash prefix."""
    prefix = generate_hash_prefix(filename, depth, chars_per_level)
    return prefix + filename


# ==============================================================================
# Test File Generation
# ==============================================================================
def generate_test_files(directory: str, count: int = 100) -> List[str]:
    """Generate test files in the specified directory."""
    os.makedirs(directory, exist_ok=True)
    files = []

    extensions = ['.parquet', '.csv', '.json', '.txt', '.xml']

    for i in range(count):
        ext = random.choice(extensions)
        filename = f"testfile_{i:04d}{ext}"
        filepath = os.path.join(directory, filename)

        # Generate random content
        content = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(100, 1000)))

        with open(filepath, 'w') as f:
            f.write(content)

        files.append(filepath)

    return files


# ==============================================================================
# Azure Operations
# ==============================================================================
def resource_group_exists(
    client: ResourceManagementClient,
    resource_group: str
) -> bool:
    """Check if resource group exists."""
    return client.resource_groups.check_existence(resource_group)


def create_resource_group(
    client: ResourceManagementClient,
    resource_group: str,
    location: str
) -> None:
    """Create a resource group."""
    client.resource_groups.create_or_update(
        resource_group,
        {"location": location}
    )


def list_resource_groups(client: ResourceManagementClient) -> List[str]:
    """List all resource groups."""
    return [rg.name for rg in client.resource_groups.list()]


def storage_account_exists(
    client: StorageManagementClient,
    resource_group: str,
    account_name: str
) -> bool:
    """Check if storage account exists in the resource group."""
    try:
        client.storage_accounts.get_properties(resource_group, account_name)
        return True
    except ResourceNotFoundError:
        return False


def list_storage_accounts(
    client: StorageManagementClient,
    resource_group: str
) -> List[str]:
    """List storage accounts in a resource group."""
    return [acc.name for acc in client.storage_accounts.list_by_resource_group(resource_group)]


def create_storage_account(
    client: StorageManagementClient,
    resource_group: str,
    account_name: str,
    location: str,
    sku_name: str = "Standard_LRS",
    enable_hns: bool = True,
    min_tls_version: str = "TLS1_2",
    public_network_access: str = "Enabled"
) -> None:
    """Create storage account with configurable settings."""
    params = StorageAccountCreateParameters(
        sku=Sku(name=sku_name),
        kind=Kind.STORAGE_V2,
        location=location,
        is_hns_enabled=enable_hns,
        enable_https_traffic_only=True,
        minimum_tls_version=min_tls_version,
        allow_blob_public_access=False,
        public_network_access=public_network_access,
    )
    poller = client.storage_accounts.begin_create(resource_group, account_name, params)
    poller.result()


def container_exists(
    blob_service: BlobServiceClient,
    container_name: str
) -> bool:
    """Check if container exists."""
    try:
        container_client = blob_service.get_container_client(container_name)
        container_client.get_container_properties()
        return True
    except ResourceNotFoundError:
        return False


def list_containers(blob_service: BlobServiceClient) -> List[str]:
    """List all containers in the storage account."""
    return [c.name for c in blob_service.list_containers()]


def create_container(
    blob_service: BlobServiceClient,
    container_name: str
) -> None:
    """Create container with private access (no public access)."""
    try:
        blob_service.create_container(name=container_name)
    except ResourceExistsError:
        pass


def upload_file_with_prefix(
    blob_service: BlobServiceClient,
    container_name: str,
    local_path: str,
    depth: int = 2,
    chars_per_level: int = 2
) -> Tuple[str, str]:
    """
    Upload a file with hash prefix to improve partition distribution.

    Returns:
        Tuple of (local_path, blob_name)
    """
    filename = os.path.basename(local_path)
    blob_name = get_prefixed_blob_name(filename, depth, chars_per_level)

    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

    with open(local_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)

    return (local_path, blob_name)


def upload_directory(
    blob_service: BlobServiceClient,
    container_name: str,
    source_dir: str,
    depth: int = 2,
    chars_per_level: int = 2,
    progress_callback=None
) -> List[Tuple[str, str]]:
    """
    Upload all files from a directory with hash prefixes.

    Returns:
        List of (local_path, blob_name) tuples
    """
    uploaded = []
    files = [f for f in Path(source_dir).iterdir() if f.is_file()]
    total = len(files)

    for i, filepath in enumerate(files):
        result = upload_file_with_prefix(
            blob_service,
            container_name,
            str(filepath),
            depth,
            chars_per_level
        )
        uploaded.append(result)

        if progress_callback:
            progress_callback(i + 1, total, filepath.name)

    return uploaded


# ==============================================================================
# Main
# ==============================================================================
def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Azure Blob Storage Hash-Prefix Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --subscription-id abc123 --resource-group my-rg
  %(prog)s -s abc123 -g my-rg
        """
    )
    parser.add_argument(
        "-s", "--subscription-id",
        required=True,
        help="Azure subscription ID"
    )
    parser.add_argument(
        "-g", "--resource-group",
        required=True,
        help="Azure resource group name (must already exist)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    subscription_id = args.subscription_id
    resource_group = args.resource_group

    print(f"""
{Colors.AZURE_LIGHT}              _   _                              _   _
{Colors.AZURE_MID}          ,--'   '--,                          ,--'   '--,
{Colors.AZURE_LIGHT}       .-'          '-.______________________.-'          '-.
{Colors.AZURE_MID}      (                                                        )
{Colors.AZURE_ACCENT}     (     ⚡  {Colors.AZURE_LIGHT}Azure Blob Storage Hash-Prefix Generator{Colors.AZURE_ACCENT}  ⚡     )
{Colors.AZURE_MID}      (                        📦 🔗 ☁️                        )
{Colors.AZURE_LIGHT}       '-._                                              _.-'
{Colors.AZURE_MID}           '---------------------------------------------'{Colors.RESET}
""")

    print(f"  {Colors.AZURE_LIGHT}Configuration{Colors.RESET}")
    print(f"  {Colors.DIM}Subscription: {subscription_id}{Colors.RESET}")
    print(f"  {Colors.DIM}Resource Group: {resource_group}{Colors.RESET}\n")

    # ---------------------------------------------------------------------------
    # Authenticate
    # ---------------------------------------------------------------------------
    sp = Spinner("Authenticating with Azure")
    sp.start()
    try:
        credential = DefaultAzureCredential()
        resource_client = ResourceManagementClient(credential, subscription_id)
        storage_client = StorageManagementClient(credential, subscription_id)
        sp.stop(success=True, message="Authenticated")
    except Exception as e:
        sp.stop(success=False, message=f"Authentication failed: {e}")
        sys.exit(1)

    # ---------------------------------------------------------------------------
    # Verify Resource Group Exists
    # ---------------------------------------------------------------------------
    sp = Spinner(f"Verifying resource group '{resource_group}'")
    sp.start()
    if not resource_group_exists(resource_client, resource_group):
        sp.stop(success=False, message=f"Resource group '{resource_group}' not found")
        print(f"  {Colors.ERROR}! The specified resource group does not exist.{Colors.RESET}")
        print(f"  {Colors.DIM}Create it first with: az group create --name {resource_group} --location <region>{Colors.RESET}")
        sys.exit(1)
    sp.stop(success=True)
    rg_location = None  # Will get from storage account if needed

    # ---------------------------------------------------------------------------
    # Storage Account
    # ---------------------------------------------------------------------------
    sa_action = prompt_create_or_use("Storage Account")

    if sa_action == "existing":
        # List existing storage accounts
        sp = Spinner("Fetching storage accounts")
        sp.start()
        sa_list = list_storage_accounts(storage_client, resource_group)
        sp.stop(success=True, message=f"Found {len(sa_list)} storage accounts")

        if sa_list:
            print(f"\n  {Colors.DIM}Available storage accounts:{Colors.RESET}")
            for sa in sa_list:
                print(f"    {Colors.AZURE_ACCENT}-{Colors.RESET} {sa}")
            print()

        account_name = prompt_storage_account_name("Storage Account Name")

        # Verify it exists
        if not storage_account_exists(storage_client, resource_group, account_name):
            print(f"  {Colors.WARN}! Storage account '{account_name}' not found{Colors.RESET}")
            if prompt_yes_no("Create it?", True):
                sa_action = "create"  # Fall through to create logic
            else:
                print(f"  {Colors.ERROR}! Cannot continue without a storage account{Colors.RESET}")
                sys.exit(1)
        else:
            sa_created = False
    else:
        account_name = prompt_storage_account_name("Storage Account Name")

    if sa_action == "create":
        # Storage account settings
        print(f"\n  {Colors.AZURE_LIGHT}Storage Account Settings{Colors.RESET}")

        enable_hns = prompt_yes_no("Enable Hierarchical Namespace (ADLS Gen2)", True)
        sku_name = prompt_choice("SKU/Redundancy", SKU_OPTIONS, "1")

        if rg_location is None:
            location = prompt("Region", "eastus2")
        else:
            location = rg_location

        # Check if already exists
        if storage_account_exists(storage_client, resource_group, account_name):
            print(f"  {Colors.INFO}Storage account '{account_name}' already exists{Colors.RESET}")
            sa_created = False
        else:
            sp = Spinner(f"Creating storage account '{account_name}'")
            sp.start()
            try:
                create_storage_account(
                    storage_client, resource_group, account_name, location,
                    sku_name=sku_name,
                    enable_hns=enable_hns
                )
                sp.stop(success=True, message=f"Created storage account '{account_name}'")
                sa_created = True
            except Exception as e:
                sp.stop(success=False, message=f"Failed: {e}")
                sys.exit(1)

    # ---------------------------------------------------------------------------
    # Connect to Blob Service
    # ---------------------------------------------------------------------------
    account_url = f"https://{account_name}.blob.core.windows.net"

    # Try to get storage account keys for data plane access
    # This avoids requiring separate RBAC data plane permissions
    sp = Spinner("Getting storage account keys")
    sp.start()
    try:
        keys = storage_client.storage_accounts.list_keys(resource_group, account_name)
        account_key = keys.keys[0].value
        blob_service = BlobServiceClient(account_url=account_url, credential=account_key)
        sp.stop(success=True, message="Using account key for blob access")
    except Exception as e:
        # Fall back to DefaultAzureCredential if we can't get keys
        sp.stop(success=False, message=f"Could not get keys, using credential: {e}")
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)

    # ---------------------------------------------------------------------------
    # Container
    # ---------------------------------------------------------------------------
    container_action = prompt_create_or_use("Container")

    if container_action == "existing":
        # List existing containers
        sp = Spinner("Fetching containers")
        sp.start()
        container_list = list_containers(blob_service)
        sp.stop(success=True, message=f"Found {len(container_list)} containers")

        if container_list:
            print(f"\n  {Colors.DIM}Available containers:{Colors.RESET}")
            for c in container_list:
                print(f"    {Colors.AZURE_ACCENT}-{Colors.RESET} {c}")
            print()

        container_name = prompt_container_name("Container Name")

        # Verify it exists
        if not container_exists(blob_service, container_name):
            print(f"  {Colors.WARN}! Container '{container_name}' not found{Colors.RESET}")
            if prompt_yes_no("Create it?", True):
                container_action = "create"
            else:
                print(f"  {Colors.ERROR}! Cannot continue without a container{Colors.RESET}")
                sys.exit(1)
        else:
            container_created = False
    else:
        container_name = prompt_container_name("Container Name", "data")

    if container_action == "create":
        if container_exists(blob_service, container_name):
            print(f"  {Colors.INFO}Container '{container_name}' already exists{Colors.RESET}")
            container_created = False
        else:
            sp = Spinner(f"Creating container '{container_name}'")
            sp.start()
            try:
                create_container(blob_service, container_name)
                sp.stop(success=True, message=f"Created container '{container_name}'")
                container_created = True
            except Exception as e:
                sp.stop(success=False, message=f"Failed: {e}")
                sys.exit(1)

    # ---------------------------------------------------------------------------
    # Hash Prefix Configuration
    # ---------------------------------------------------------------------------
    print(f"\n  {Colors.AZURE_LIGHT}Hash Prefix Settings{Colors.RESET}")
    print(f"  {Colors.DIM}Hash prefixes improve partition distribution (e.g., aa/bb/file.txt){Colors.RESET}\n")

    prefix_depth = prompt_int("Prefix Depth (directory levels)", 2, 1, 8)
    chars_per_level = prompt_int("Characters Per Level", 2, 1, 4)

    # Show example
    example_prefix = generate_hash_prefix("example_file.parquet", prefix_depth, chars_per_level)
    print(f"\n  {Colors.DIM}Example: example_file.parquet -> {example_prefix}example_file.parquet{Colors.RESET}")

    # ---------------------------------------------------------------------------
    # Source Directory
    # ---------------------------------------------------------------------------
    print(f"\n  {Colors.AZURE_LIGHT}Upload Configuration{Colors.RESET}")

    source_dir = prompt("Source Directory (or press Enter to generate test files)", "", required=False)

    if not source_dir:
        # Generate test files
        temp_dir = tempfile.mkdtemp(prefix="azure_test_")
        print(f"\n  {Colors.INFO}Generating 100 test files in {temp_dir}{Colors.RESET}")

        sp = Spinner("Generating test files")
        sp.start()
        test_files = generate_test_files(temp_dir, 100)
        sp.stop(success=True, message=f"Generated {len(test_files)} test files")
        source_dir = temp_dir
    else:
        # Validate directory exists
        if not os.path.isdir(source_dir):
            print(f"  {Colors.ERROR}! Directory not found: {source_dir}{Colors.RESET}")
            sys.exit(1)

        # Check for files
        files = [f for f in Path(source_dir).iterdir() if f.is_file()]
        if not files:
            print(f"  {Colors.WARN}! No files found in {source_dir}{Colors.RESET}")
            if prompt_yes_no("Generate 100 test files instead?", True):
                sp = Spinner("Generating test files")
                sp.start()
                test_files = generate_test_files(source_dir, 100)
                sp.stop(success=True, message=f"Generated {len(test_files)} test files")
            else:
                print(f"  {Colors.ERROR}! No files to upload{Colors.RESET}")
                sys.exit(1)

    # ---------------------------------------------------------------------------
    # Upload Files
    # ---------------------------------------------------------------------------
    files_to_upload = [f for f in Path(source_dir).iterdir() if f.is_file()]
    total_files = len(files_to_upload)

    print(f"\n  {Colors.AZURE_LIGHT}Uploading {total_files} files with hash prefixes{Colors.RESET}")
    print(f"  {Colors.DIM}Using overwrite=True for idempotent uploads{Colors.RESET}\n")

    if prompt_yes_no("Proceed with upload?", True):
        uploaded_files = []
        errors = []

        for i, filepath in enumerate(files_to_upload):
            progress = f"[{i + 1}/{total_files}]"
            sys.stdout.write(f"\r  {Colors.AZURE_ACCENT}{progress}{Colors.RESET} Uploading {filepath.name[:40]}...")
            sys.stdout.flush()

            try:
                result = upload_file_with_prefix(
                    blob_service,
                    container_name,
                    str(filepath),
                    prefix_depth,
                    chars_per_level
                )
                uploaded_files.append(result)
            except Exception as e:
                errors.append((filepath.name, str(e)))

        # Clear the line
        sys.stdout.write('\r' + ' ' * 80 + '\r')
        sys.stdout.flush()

        if uploaded_files:
            print(f"  {Colors.SUCCESS}+ Uploaded {len(uploaded_files)} files{Colors.RESET}")

        if errors:
            print(f"  {Colors.ERROR}! {len(errors)} files failed to upload{Colors.RESET}")
            for filename, error in errors[:5]:
                print(f"    {Colors.DIM}{filename}: {error}{Colors.RESET}")
    else:
        uploaded_files = []
        print(f"  {Colors.INFO}Upload skipped{Colors.RESET}")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    print(f"\n  {Colors.AZURE_MID}{'=' * 50}{Colors.RESET}")
    print(f"  {Colors.AZURE_LIGHT}Summary{Colors.RESET}\n")

    print(f"  {Colors.AZURE_MID}Resource Group{Colors.RESET}   {resource_group}")
    print(f"  {Colors.AZURE_MID}Storage Account{Colors.RESET}  {account_name}")
    print(f"  {Colors.AZURE_MID}Container{Colors.RESET}        {container_name}")
    print(f"  {Colors.AZURE_MID}Prefix Format{Colors.RESET}    {prefix_depth} levels, {chars_per_level} chars each")

    if uploaded_files:
        print(f"\n  {Colors.AZURE_MID}Sample uploaded blobs:{Colors.RESET}")
        for local_path, blob_name in uploaded_files[:5]:
            print(f"    {Colors.AZURE_ACCENT}->{Colors.RESET} {blob_name}")
        if len(uploaded_files) > 5:
            print(f"    {Colors.DIM}... and {len(uploaded_files) - 5} more{Colors.RESET}")

    print(f"\n  {Colors.SUCCESS}Done.{Colors.RESET}\n")


if __name__ == "__main__":
    main()
