# Contributing to SQL Testing Library

## Dependabot Integration Tests

For security reasons, integration tests are **skipped for Dependabot PRs**. Unit tests still run normally.
## Pull Request Title Format

All PR titles MUST follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>[optional scope]: <description>
```

**Note:** Individual commits within a PR do not need to follow this format. Only the PR title is validated.

### PR Title Types

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
| `perf` | Performance improvements | Patch |
| `build` | Build system or external dependencies | None |
| `revert` | Reverts a previous commit | Varies |

### Examples

✅ **Good PR Title Examples:**
- `feat: add BigQuery adapter support`
- `fix: resolve connection timeout in Athena adapter`
- `docs: update setup instructions for BigQuery`
- `feat(core): implement table validation`
- `fix(adapters): handle null values in mock tables`
- `test: add integration tests for Snowflake`
- `chore: update dependencies to latest versions`

❌ **Bad PR Title Examples:**
- `Added new feature`
- `Bug fix`
- `Updated docs`
- `Fixed issue`
- `Improvements`
- `WIP`

### Breaking Changes

For breaking changes in your PR title, add `!` after the type:

```
feat!: redesign adapter interface
fix!: change return type of validate_table
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

PR titles are automatically validated by GitHub Actions when you:
- Open a new PR
- Edit the PR title
- Push new commits

Invalid PR titles will fail the "Check PR title" workflow with a helpful error message explaining the correct format.

### Pull Request Guidelines

1. Your PR title must follow conventional commit format (individual commits don't need to)
2. Use a descriptive PR title that explains what the PR accomplishes
3. The PR title will be used as the merge commit message
4. Keep PRs focused on a single feature or fix

### Need Help?

- Review existing PR titles for examples
- Use the conventional commits specification: https://www.conventionalcommits.org/
- The PR title validation workflow will provide specific guidance if your title is invalid

## Individual Commit Messages

Individual commits within your PR **do not** need to follow conventional commit format. Feel free to use informal commit messages like:
- "WIP"
- "fix typo"
- "address review comments"
- "update tests"

Only the PR title matters for our automated tooling and changelog generation.
