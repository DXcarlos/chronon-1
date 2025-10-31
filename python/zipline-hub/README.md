# Zipline Hub

This package extends `zipline-ai` with Zipline Hub integration functionality, enabling seamless interaction with the Zipline Hub service for managing, uploading, and deploying Chronon configurations.

## What is Zipline Hub?

Zipline Hub is a centralized service for managing Chronon feature definitions, metadata, and orchestration. This package provides the client-side tools to interact with Hub instances.

## Installation

### Option 1: Install as an extra (Recommended)

```bash
pip install zipline-ai[hub]
```

This automatically installs both `zipline-ai` and `zipline-hub` together.

### Option 2: Install separately

```bash
pip install zipline-ai
pip install zipline-hub
```

### Local Development

For local development with editable installs:

```bash
# Install both packages in editable mode
pip install -e python/[hub]

# Or install them separately
pip install -e python/
pip install -e python/zipline-hub/
```

## Requirements

### Core Dependency
- **zipline-ai** >= 0.0.1 (automatically installed)

### Additional Dependencies
- **requests** >= 2.31.0 - HTTP client for Hub API communication
- **google-auth** >= 2.23.0 - Google Cloud authentication
- **google-cloud-iam** >= 2.12.0 - IAM credentials management for service account authentication

### Python Version
- Python >= 3.11

## What Does This Package Add?

When installed alongside `zipline-ai`, this package extends functionality with:

### 1. **Hub CLI Commands**
The `zipline hub` command group for interacting with Zipline Hub:
```bash
zipline hub --help
```

### 2. **ZiplineHub Client**
Python client for programmatic Hub API access with Google Cloud authentication:
```python
from ai.chronon.repo.zipline_hub import ZiplineHub

# Initialize with authentication
hub = ZiplineHub(
    base_url="https://hub.example.com",
    sa_name="service-account-name",  # Optional
    use_auth=True
)
```

### 3. **Hub Upload & Sync**
Tools for uploading and synchronizing Chronon configurations to Hub:
- Configuration upload
- Metadata synchronization
- Version management

### 4. **Hub Runner**
Execution framework for Hub-based deployments and workflows.

## Authentication

The package supports multiple authentication methods:

1. **Environment Variable**: Set `GCP_ID_TOKEN` (used in CI/CD)
2. **Service Account**: Specify via `sa_name` parameter
3. **Default Credentials**: Uses Google Cloud Application Default Credentials

For HTTPS endpoints, authentication is automatically enabled.

## Namespace Package

This package uses Python namespace packages to extend the `ai.chronon` namespace seamlessly:

```python
# Base functionality - works with just zipline-ai
from ai.chronon import model, query

# Hub functionality - requires zipline-hub
from ai.chronon.repo.zipline_hub import ZiplineHub
from ai.chronon.repo import hub_runner, hub_uploader
```

## Package Structure

```
ai/
└── chronon/
    └── repo/
        ├── zipline_hub.py      # Hub client with authentication
        ├── hub_runner.py       # CLI commands and execution
        └── hub_uploader.py     # Upload functionality
```

## License

Copyright (C) 2023 The Chronon Authors.
Licensed under the Apache License, Version 2.0.
