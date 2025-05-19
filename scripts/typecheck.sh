#!/bin/bash
# Run mypy type checking

set -e

echo "Running Mypy type checking..."
poetry run mypy --ignore-missing-imports --disallow-untyped-defs --disallow-incomplete-defs src

echo "Type checking complete!"
