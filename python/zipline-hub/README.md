# Zipline Hub

This package extends `zipline-ai` with Zipline Hub integration functionality.

## Installation

This is an addon package that requires `zipline-ai` to be installed:

```bash
# Install the core package first
pip install zipline-ai

# Then install the hub addon
pip install zipline-hub
```

Or install both together:

```bash
pip install zipline-ai zipline-hub
```

## Features

When installed alongside `zipline-ai`, this package adds:

- `zipline hub` CLI commands for interacting with Zipline Hub
- Hub upload and sync functionality
- Hub-based deployment and management tools

## Usage

After installation, the `zipline hub` command group becomes available:

```bash
zipline hub --help
```

## Namespace Package

This package uses Python namespace packages to extend the `ai.chronon` namespace.
All imports remain the same whether you have just `zipline-ai` or both packages installed:

```python
from ai.chronon import model, query  # Works with just zipline-ai
from ai.chronon.repo.zipline_hub import ZiplineHub  # Works when zipline-hub is installed
```

## License

Copyright (C) 2023 The Chronon Authors.
Licensed under the Apache License, Version 2.0.
