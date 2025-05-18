#!/bin/bash
# Lint the codebase with Ruff

set -e

echo "Running Ruff linter..."
poetry run ruff check src tests

echo "Running Ruff formatter (checking only)..."
poetry run ruff format --check src tests

echo "To apply formatting automatically, run: poetry run ruff format src tests"