#!/bin/bash
# Run tests with code coverage

set -e

echo "Running tests with code coverage..."
poetry run pytest --cov=src/sql_testing_library --cov-report=term-missing --cov-report=html

echo "Coverage report generated in htmlcov/index.html"
echo "To view the report, open htmlcov/index.html in a browser"
