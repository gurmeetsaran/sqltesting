#!/bin/bash
# Setup pre-commit hooks for the project

set -e

echo "Installing pre-commit hooks..."

# Make sure pre-commit is installed
if ! command -v poetry &> /dev/null; then
    echo "Poetry not found. Please install poetry first."
    exit 1
fi

# Install dev dependencies
echo "Installing dev dependencies..."
poetry install --with dev

# Install the pre-commit hooks
echo "Installing git hooks..."
poetry run pre-commit install

echo "Pre-commit hooks installed successfully!"
echo "Now your code will be automatically checked and formatted when you commit."
echo ""
echo "To run the hooks manually on all files:"
echo "  poetry run pre-commit run --all-files"
