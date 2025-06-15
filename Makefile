.PHONY: help install test test-unit test-integration test-all test-tox lint format check clean build docs

# Default target - show help
help:
	@echo "Available commands:"
	@echo "  make install          Install dependencies with poetry"
	@echo "  make test            Run unit tests"
	@echo "  make test-unit       Run unit tests with coverage"
	@echo "  make test-integration Run integration tests (requires DB credentials)"
	@echo "  make test-all        Run all tests (unit + integration)"
	@echo "  make test-tox        Run tests with tox (all Python versions)"
	@echo "  make lint            Run linting checks (ruff + mypy)"
	@echo "  make format          Format code with black and ruff"
	@echo "  make check           Run all checks (lint + format check + tests)"
	@echo "  make clean           Remove build artifacts and cache files"
	@echo "  make build           Build distribution packages"
	@echo "  make docs            Build documentation"

# Install dependencies
install:
	poetry install --all-extras

# Run unit tests
test: test-unit

# Run unit tests with coverage
test-unit:
	poetry run pytest -m "not integration" --cov=src/sql_testing_library --cov-report=term-missing

# Run integration tests
test-integration:
	poetry run pytest -m "integration" --cov=src/sql_testing_library --cov-report=term-missing

# Run all tests
test-all:
	poetry run pytest --cov=src/sql_testing_library --cov-report=term-missing

# Run tests with tox (all Python versions)
test-tox:
	tox

# Run linting
lint:
	poetry run ruff check src tests
	poetry run mypy src

# Format code
format:
	poetry run black src tests
	poetry run ruff check --fix src tests

# Check formatting without modifying files
format-check:
	poetry run black --check src tests
	poetry run ruff check src tests

# Run all checks
check: format-check lint test-unit

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf .pytest_cache/
	rm -rf .tox/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Build distribution packages
build: clean
	poetry build

# Build documentation
docs:
	@echo "Building documentation..."
	@if [ -d "docs" ]; then \
		poetry run sphinx-build -b html docs docs/_build/html; \
	else \
		echo "No docs directory found. Skipping documentation build."; \
	fi

# Development shortcuts
.PHONY: t l f
t: test
l: lint
f: format
