# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Feat

- **adapters**: add MAP datatype support for Athena and Trino adapters
  - Support for `Dict[K, V]` Python types mapping to SQL `MAP(K_TYPE, V_TYPE)`
  - Proper NULL map handling with type-specific casting
  - Empty map support
  - Comprehensive integration tests for various map scenarios

## 0.7.1 (2025-06-06)

### Fix

- **array**: array handling logic + sql logging improvement (#95)

## 0.7.0 (2025-06-06)

### Feat

- **sqllogging**: added support for logging sql logs for debugging failed tests (#94)

## 0.6.0 (2025-06-05)

### Feat

- migrate from mypy to pyright for type checking (#86)

### Perf

- **athena**: upgrade sqlglot package to get athena dialect (#88)

## 0.5.0 (2025-06-01)

### BREAKING CHANGE

- SQLTestCase parameter execution_database renamed to default_namespace for clarity

### Feat

- rename execution_database to default_namespace and standardize database context handling (#84)

## 0.4.0 (2025-05-31)

### Feat

- implement lazy loading for heavy dependencies to improve import… (#82)
- add comprehensive GitHub issue templates (#80)
- parametrize integration tests and standardize SQL across adapters (#75)
- add comprehensive test coverage for core framework functionality (#68)
- enhance Codecov reporting and fix Redshift namespace deletion wait (#67)
- add comprehensive Pydantic model support for mock data (#66)
- add comprehensive array support and enhance Redshift security cleanup (#64)
- unify SQL string escaping and fix database-specific issues (#62)

### Fix

- update all test imports after private module refactoring (#69)

## 0.3.0 (2025-05-26)

### Feat

- add comprehensive Snowflake integration support (#61)
- add comprehensive Trino integration testing with Docker support

## 0.2.1 (2025-05-26)

### Fix

- fix for change log generation

## 0.2.0 (2025-05-26)

### Feat

- add comprehensive Redshift integration testing with Serverless support

### Fix

- correct commitizen command syntax in release workflow
- update commitizen arguments in release workflow
- add explicit type casting for NULL values in Redshift adapter
- resolve Python version compatibility issues

## 0.1.3 (2025-05-25)

### Fix

- allow triggering tests from release yaml file
- improvement in ci
- make release process manual

## 0.1.2 (2025-05-25)

### Fix

- use personal access token for GitHub release creation

## 0.1.1 (2025-05-25)

### Fix

- use pat to push release version
- release bump version was failing when there are no change

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
