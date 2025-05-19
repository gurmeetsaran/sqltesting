#!/bin/bash
# Lint the codebase with Ruff and Mypy

set -e

echo "Running Ruff linter..."
poetry run ruff check src tests

echo "Running Ruff formatter (checking only)..."
poetry run ruff format --check src tests

echo "Running Mypy type checking..."
poetry run mypy src

echo "To apply formatting automatically, run: poetry run ruff format src tests"
