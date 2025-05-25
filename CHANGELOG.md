# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- BigQuery integration tests with comprehensive test coverage
- Automated commit message validation using conventional commits
- Pre-commit hooks for code quality enforcement
- GitHub Actions workflow for commit validation
- Dedicated BigQuery CI/CD workflow with environment setup
- Documentation for BigQuery setup and linting guidelines
- Validation scripts for BigQuery and Athena environments

### Changed
- Simplified main test workflow by removing BigQuery credentials
- Enhanced linting configuration with consistent exclusions
- Updated mypy configuration for better type checking

### Fixed
- Type annotation issues in Snowflake adapter
- Linting configuration inconsistencies across tools

## [0.1.0] - Initial Release

### Added
- Core SQL testing framework with mock table injection
- Support for multiple database adapters:
  - BigQuery
  - Athena
  - Redshift
  - Snowflake
  - Trino
- Pytest plugin integration
- Comprehensive test suite
- Documentation and setup guides
