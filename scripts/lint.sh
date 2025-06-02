#!/bin/bash
# Lint the codebase with Ruff and Pyright

set -e

echo "Running Ruff linter..."
poetry run ruff check src tests --exclude scripts/

echo "Running Ruff formatter (checking only)..."
poetry run ruff format --check src tests --exclude scripts/

echo "Running Pyright type checking..."
poetry run pyright

echo "✅ All lint checks passed!"
echo ""
echo "To apply formatting automatically, run: poetry run ruff format src tests"
