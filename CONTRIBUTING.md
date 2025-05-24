# Contributing to SQL Testing Library

## Commit Message Format

All commits MUST follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Commit Types

| Type | Description | Version Impact |
|------|-------------|----------------|
| `feat` | New feature | Minor (1.2.0 → 1.3.0) |
| `fix` | Bug fix | Patch (1.2.0 → 1.2.1) |
| `docs` | Documentation only | None |
| `style` | Code style changes (formatting, etc.) | None |
| `refactor` | Code refactoring (no functionality change) | None |
| `test` | Adding or updating tests | None |
| `chore` | Maintenance tasks (dependencies, etc.) | None |
| `ci` | CI/CD changes | None |

### Examples

✅ **Good Examples:**
```bash
feat: add BigQuery adapter support
fix: resolve connection timeout in Athena adapter
docs: update setup instructions for BigQuery
feat(core): implement table validation
fix(adapters): handle null values in mock tables
test: add integration tests for Snowflake
chore: update dependencies to latest versions
```

❌ **Bad Examples:**
```bash
Added new feature
Bug fix
Updated docs
Fixed issue
Improvements
WIP
```

### Breaking Changes

For breaking changes, use one of these formats:

**Method 1: Add `!` after type**
```bash
feat!: redesign adapter interface
fix!: change return type of validate_table
```

**Method 2: Use `BREAKING CHANGE:` footer**
```bash
feat: redesign adapter interface

BREAKING CHANGE: Adapter.connect() now requires credentials parameter
```

Breaking changes trigger a major version bump (1.2.0 → 2.0.0).

### Scopes (Optional)

Use scopes to specify the area of change:
- `core`: Core functionality
- `adapters`: Database adapters
- `bigquery`: BigQuery-specific
- `athena`: Athena-specific
- `snowflake`: Snowflake-specific
- `redshift`: Redshift-specific
- `trino`: Trino-specific
- `tests`: Test-related changes
- `docs`: Documentation changes

### Enforcement

This repository uses multiple layers of validation:
1. **Pre-commit hooks** - Block invalid commits locally
2. **GitHub Actions** - Validate all commits in PRs using Commitizen

Invalid commits will be rejected with helpful error messages.

To set up pre-commit hooks:
```bash
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```

### Pull Request Guidelines

1. All commits in your PR must follow conventional commit format
2. Use descriptive commit messages that explain the "why"
3. Keep commits atomic (one logical change per commit)
4. Squash fixup commits before merging

### Need Help?

- Review existing commits for examples
- Use the conventional commits specification: https://www.conventionalcommits.org/
- Ask in pull request comments if unsure about commit message format
