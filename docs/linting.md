# Linting and Formatting Guidelines

This project uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting Python code. Ruff is a fast Python linter and formatter written in Rust.

## Setup

Ruff is already included in the dev dependencies of the project. You can install it with:

```bash
poetry install --with dev
```

## Linting

To check the codebase for linting issues:

```bash
# Using the script
./scripts/lint.sh

# Or directly
poetry run ruff check src tests
```

## Formatting

To check if the code is properly formatted:

```bash
poetry run ruff format --check src tests
```

To automatically format the code:

```bash
# Using the script
./scripts/format.sh

# Or directly
poetry run ruff format src tests
```

## Configuration

Ruff configuration is defined in the `pyproject.toml` file under the `[tool.ruff]` section. Key settings include:

- Line length is set to 88 characters (same as Black)
- Target Python version is 3.9+
- Basic rules are enabled (E, F, I, B) covering:
  - PEP 8 style guide (E)
  - Pyflakes logical errors (F)
  - Import sorting (I)
  - Bugbear checks (B)

## Pre-commit Hooks

The project is configured with pre-commit hooks to automatically check and format code before each commit. This ensures consistent code quality across the codebase.

### Installation

1. Install pre-commit (already included in dev dependencies):
   ```bash
   poetry install --with dev
   ```

2. Install the pre-commit hooks:
   ```bash
   ./scripts/setup-hooks.sh
   ```

### Usage

Once installed, the hooks will run automatically on each `git commit`. If any issues are found:

1. The commit will be aborted
2. The hooks will fix the issues they can automatically fix
3. You'll need to stage the changes and commit again

To run the hooks manually without committing:

```bash
poetry run pre-commit run --all-files
```

## IDE Integration

Ruff has extensions available for various IDEs:

- VSCode: [Ruff Extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
- PyCharm: [Ruff Plugin](https://plugins.jetbrains.com/plugin/20574-ruff)

These extensions will highlight linting issues directly in your editor and can apply automatic formatting on save.

## Development Workflow

For the best development experience:

1. Set up the pre-commit hooks as described above
2. Configure your IDE to use Ruff for linting and formatting
3. Run pre-commit hooks locally before pushing changes

This ensures that your code always meets the project's formatting and linting standards before it's committed or pushed to the repository.
