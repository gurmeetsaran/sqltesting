#!/bin/bash
# Run pyright type checking

set -e

echo "Running Pyright type checking..."
poetry run pyright

echo "Type checking complete!"
