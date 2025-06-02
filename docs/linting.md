# Linting and Formatting Guidelines

This project uses [Ruff](https://github.com/astral-sh/ruff) for linting and formatting Python code, and [Pyright](https://github.com/microsoft/pyright) for static type checking.

## Setup

Linting and type checking tools are already included in the dev dependencies of the project. You can install them with:

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

## Type Checking

To run static type checking with Pyright:

```bash
# Using the script
./scripts/typecheck.sh

# Or directly
poetry run pyright
```

## Code Coverage

The project uses [pytest-cov](https://pytest-cov.readthedocs.io/) to measure code coverage. This helps ensure that tests exercise as much of the codebase as possible.

### Local Coverage

To run tests with code coverage locally:

```bash
# Using the script
./scripts/coverage.sh

# Or directly
poetry run pytest
```

The coverage configuration is defined in `pyproject.toml` under the `[tool.pytest.ini_options]` section. The current setup:

- Measures coverage for the `src/sql_testing_library` directory
- Generates a terminal report with missing lines highlighted
- Creates an XML report at `coverage.xml` (useful for CI systems)
- Creates an HTML report in the `htmlcov` directory

To view the HTML coverage report, open `htmlcov/index.html` in a browser after running the tests.

### CI/CD Coverage

The project is also configured to publish code coverage metrics to [Codecov](https://codecov.io/) as part of the CI pipeline. This provides:

1. A public dashboard showing code coverage trends over time
2. A badge in the README showing current coverage percentage
3. Coverage feedback on pull requests
4. Detailed reports on which parts of the code lack test coverage

The GitHub Actions workflow automatically:
- Runs tests with coverage enabled
- Generates an XML coverage report
- Uploads the report to Codecov

The Codecov configuration is stored in `codecov.yml` at the root of the repository.

## Configuration

### Ruff

Ruff configuration is defined in the `pyproject.toml` file under the `[tool.ruff]` section. Key settings include:

- Line length is set to 88 characters (same as Black)
- Target Python version is 3.9+
- Basic rules are enabled (E, F, I, B) covering:
  - PEP 8 style guide (E)
  - Pyflakes logical errors (F)
  - Import sorting (I)
  - Bugbear checks (B)

### Pyright

Pyright configuration is defined in the `pyproject.toml` file under the `[tool.pyright]` section. Key settings include:

- Strict type checking enabled for the src directory
- Disallows untyped function definitions and incomplete type hints
- Warns about unused imports and variables
- Properly handles optional dependencies pattern
- Includes the src directory, excludes tests and scripts directories

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

### Ruff
Ruff has extensions available for various IDEs:

- VSCode: [Ruff Extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
- PyCharm: [Ruff Plugin](https://plugins.jetbrains.com/plugin/20574-ruff)

### Pyright
Pyright has extensions available for various IDEs:

- VSCode: [Pylance Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance) (built on Pyright)
- PyCharm: [Pyright Plugin](https://plugins.jetbrains.com/plugin/18340-pyright)

## Development Workflow

For the best development experience:

1. Set up the pre-commit hooks as described above
2. Configure your IDE to use Ruff for linting and formatting
3. Configure your IDE to use Pyright for type checking
4. Run pre-commit hooks locally before pushing changes

This ensures that your code always meets the project's formatting, linting, and type safety standards before it's committed or pushed to the repository.

## Consistency Across Tools

To ensure all linting and type checking tools work consistently:

### Configuration Files
- **`pyproject.toml`** - Primary pyright configuration under `[tool.pyright]` section
- **`.pre-commit-config.yaml`** - Must use local hook to respect project configuration
- **`scripts/lint.sh`** - Uses same pyright configuration as other tools

### External Library Handling
Optional dependencies are handled using the try/except import pattern in the code:
```python
try:
    import newlibrary
    has_newlibrary = True
except ImportError:
    has_newlibrary = False
    newlibrary = None  # type: ignore
```

### Testing Consistency
All three approaches should give the same results:
```bash
poetry run pyright          # Direct pyright
./scripts/lint.sh           # Lint script
pre-commit run pyright --all-files  # Pre-commit hook
```

### Current Exclusions
- **Scripts folder**: Excluded from all linting (allows print statements, external deps)
- **Tests folder**: Excluded from pyright (different type checking rules)
- **Source folder**: Full strict linting and type checking applied
