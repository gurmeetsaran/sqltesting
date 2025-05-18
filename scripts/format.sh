#!/bin/bash
# Automatically format the codebase with Ruff

set -e

echo "Running Ruff formatter to fix formatting issues..."
poetry run ruff format src tests

echo "Running Ruff linter with --fix option to fix linting issues..."
poetry run ruff check --fix src tests

echo "Formatting complete!"