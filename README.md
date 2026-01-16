# Azure Blob Storage Hash-Prefix Generator

Interactive CLI tool for Azure Blob Storage that uploads files with hash-based directory prefixes to improve partition distribution and performance.

## Why Hash Prefixes?

Azure Storage partitions data using a key that includes the blob name. When files have sequential or similar names, they concentrate on the same partitions, creating hotspots that limit throughput.

By prepending a hash-based prefix (e.g., `a3/f7/yourfile.parquet`), blobs are distributed evenly across partitions, improving:
- **Ingest performance** - parallel writes spread across partitions
- **List operations** - reduced latency for enumeration
- **Read throughput** - better load balancing

## Features

- **Interactive setup** - guided prompts for all Azure resources
- **Create or reuse** - works with existing storage accounts/containers or creates new ones
- **ADLS Gen2 support** - optional Hierarchical Namespace (HNS) for Data Lake workloads
- **Configurable prefixes** - customize depth and characters per level
- **Test file generation** - auto-generate sample files for testing
- **Progress feedback** - spinners and colored output

## Requirements

- Python 3.8+
- Azure CLI authenticated (`az login`) or other [DefaultAzureCredential](https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential) method
- Required packages:
  ```
  azure-identity
  azure-mgmt-resource
  azure-mgmt-storage
  azure-storage-blob
  ```

## Installation

```bash
pip install azure-identity azure-mgmt-resource azure-mgmt-storage azure-storage-blob
```

## Usage

```bash
python hdlsprefix-generator.py -s <subscription-id> -g <resource-group>
```

### Arguments

| Argument | Description |
|----------|-------------|
| `-s`, `--subscription-id` | Azure subscription ID (required) |
| `-g`, `--resource-group` | Resource group name - must already exist (required) |

### Example

```bash
# Ensure you're logged in
az login

# Run the generator
python hdlsprefix-generator.py -s "12345678-1234-1234-1234-123456789abc" -g "my-resource-group"
```

The tool will then interactively prompt you for:
1. **Storage Account** - create new or use existing
2. **SKU/Redundancy** - LRS, GRS, ZRS, etc.
3. **Container** - create new or use existing
4. **Hash prefix settings** - depth (directory levels) and characters per level
5. **Source directory** - path to files or auto-generate test files

### Hash Prefix Examples

| Settings | Input | Output |
|----------|-------|--------|
| depth=2, chars=2 | `report.parquet` | `a3/f7/report.parquet` |
| depth=3, chars=2 | `data.csv` | `b1/c4/e9/data.csv` |
| depth=1, chars=4 | `file.json` | `a3f7/file.json` |

## How It Works

1. Computes MD5 hash of the original filename
2. Extracts prefix segments from the hash (e.g., first 4 hex chars = `a3f7`)
3. Creates nested directory structure (e.g., `a3/f7/`)
4. Uploads blob with the prefixed path

The hash is deterministic - the same filename always produces the same prefix, making lookups predictable.

## License

MIT
