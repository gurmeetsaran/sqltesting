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

## IDE Integration

Ruff has extensions available for various IDEs:

- VSCode: [Ruff Extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
- PyCharm: [Ruff Plugin](https://plugins.jetbrains.com/plugin/20574-ruff)

These extensions will highlight linting issues directly in your editor and can apply automatic formatting on save.