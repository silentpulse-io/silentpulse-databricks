# Contributing to silentpulse-databricks

Thank you for your interest in contributing to silentpulse-databricks!

## Development Setup

### Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Getting Started

1. Clone the repository:

```bash
git clone https://github.com/silentpulse-io/silentpulse-databricks.git
cd silentpulse-databricks
```

2. Create a virtual environment and install all packages in editable mode:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e ./silentpulse-sdp[dev] -e ./silentpulse-datasource[dev] -e ./silentpulse-uc-discovery[dev]
```

3. Verify the installation:

```bash
python -c "from silentpulse_sdp import heartbeat; print('sdp OK')"
python -c "from silentpulse_datasource import SilentPulseDataSource; print('datasource OK')"
python -c "from silentpulse_uc_discovery import UCDiscovery; print('uc-discovery OK')"
```

## Code Standards

### Linting and formatting

We use [Ruff](https://docs.astral.sh/ruff/) pinned to version 0.11.0 for consistent results across environments.

```bash
# Lint all packages and examples
uvx ruff@0.11.0 check silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/ examples/

# Check formatting
uvx ruff@0.11.0 format --check silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/ examples/

# Auto-fix lint issues
uvx ruff@0.11.0 check --fix silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/ examples/

# Auto-format
uvx ruff@0.11.0 format silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/ examples/
```

Ruff configuration per subpackage (`pyproject.toml`):
- Line length: 120
- Target version: Python 3.9
- Lint rules: E (pycodestyle errors), F (pyflakes), I (isort), B (bugbear)

### Python version compatibility

The project targets **Python 3.9+** to support older Databricks Runtimes. Follow these conventions:

- Always add `from __future__ import annotations` at the top of modules that use type hints. This enables modern syntax (`list[str]`, `dict | None`) on Python 3.9.
- Do NOT use runtime features from Python 3.10+ (e.g., `match/case`, `ParamSpec` from `typing`, `tomllib`).
- Keep `requires-python = ">=3.9"` in all `pyproject.toml` files.

### Tests

Each subpackage has its own `tests/` directory. Run tests per subpackage:

```bash
cd silentpulse-sdp && pytest -v
cd silentpulse-datasource && pytest -v
cd silentpulse-uc-discovery && pytest -v
```

Or all at once from the repo root:

```bash
pytest silentpulse-sdp/tests/ silentpulse-datasource/tests/ silentpulse-uc-discovery/tests/ -v
```

## Subpackage Structure

Each subpackage is independent with its own `pyproject.toml`, source directory, and tests:

```
silentpulse-<name>/           # Package directory (pip installable)
  pyproject.toml              # Build config, dependencies, tool config
  silentpulse_<name>/         # Python package (importable)
    __init__.py               # Version + public API exports
    <module>.py               # Implementation
  tests/
    __init__.py
    test_<module>.py           # Tests
```

When adding a new module:

1. Place source code in the corresponding `silentpulse_<name>/` directory
2. Add tests in the `tests/` directory within the subpackage
3. Update the package's `__init__.py` exports if adding public API
4. Update `__all__` in `__init__.py` to list all public names

## Version Management

Version `0.1.0` is currently defined in multiple locations that must stay in sync:

| File | Format |
|------|--------|
| `VERSION` (root) | Plain text: `0.1.0` |
| `silentpulse-sdp/pyproject.toml` | `version = "0.1.0"` |
| `silentpulse-sdp/silentpulse_sdp/__init__.py` | `__version__ = "0.1.0"` |
| `silentpulse-datasource/pyproject.toml` | `version = "0.1.0"` |
| `silentpulse-datasource/silentpulse_datasource/__init__.py` | `__version__ = "0.1.0"` |
| `silentpulse-uc-discovery/pyproject.toml` | `version = "0.1.0"` |
| `silentpulse-uc-discovery/silentpulse_uc_discovery/__init__.py` | `__version__ = "0.1.0"` |

When bumping the version, update all seven locations. All packages share the same version number.

## Decorator Patterns

SilentPulse SDP decorators follow the parameterized decorator pattern:

```python
def my_decorator(*, param: str) -> Callable:
    """Outer function: receives configuration."""
    def decorator(func: Callable) -> Callable:
        """Middle function: receives the decorated function."""
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Inner function: called at runtime."""
            # Pre-processing (e.g., start timer)
            result = func(*args, **kwargs)
            # Post-processing (e.g., report telemetry)
            return result
        return wrapper
    return decorator
```

When stacking with `@dlt.table`, SilentPulse decorators must go **below** it:

```python
@dlt.table(...)     # Outermost: applied last, registers with DLT
@heartbeat(...)     # Applied first, wraps the raw function
def my_table():
    ...
```

This ensures DLT receives the wrapped function and calls through the SilentPulse layer during execution.

## Databricks Runtime Compatibility

| Feature | Minimum DBR | Notes |
|---------|-------------|-------|
| Spark Declarative Pipelines (SDP) | 14.2 | Required for `@dlt.table` |
| PySpark DataSource API v2 | 14.2 | Required for custom `spark.read.format(...)` |
| Unity Catalog | 13.3+ | Required for UC Discovery |
| Databricks SDK | Any | UC Discovery uses REST API, not Spark |

Test your changes on the minimum supported DBR version when possible.

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear, focused commits
3. Ensure linting passes: `uvx ruff@0.11.0 check ...`
4. Ensure formatting passes: `uvx ruff@0.11.0 format --check ...`
5. Add or update tests for changed functionality
6. Open a PR against `main` with a clear description
7. Wait for CI to pass and a maintainer review

### Commit message conventions

Use conventional commit prefixes:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation only
- `refactor:` - Code changes that neither fix a bug nor add a feature
- `test:` - Adding or updating tests
- `ci:` - CI/CD changes

## Security

- Never commit secrets, tokens, or credentials
- Use Databricks Secrets for API tokens in examples
- The `.gitignore` excludes `.env` files, but always double-check before committing

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
