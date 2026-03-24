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

2. Create a virtual environment and install packages in editable mode:

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e ./silentpulse-sdp[dev] -e ./silentpulse-datasource[dev] -e ./silentpulse-uc-discovery[dev]
```

3. Verify the installation:

```bash
python -c "from silentpulse_sdp import heartbeat; print('OK')"
```

## Code Standards

- **Linting and formatting**: [Ruff](https://docs.astral.sh/ruff/) with line length 120
- **Python version**: 3.9+ (use `from __future__ import annotations` for modern type hints)
- **Tests**: pytest, one test directory per subpackage

### Running Linters

```bash
uvx ruff@0.11.0 check silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/
uvx ruff@0.11.0 format --check silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/
```

### Running Tests

```bash
cd silentpulse-sdp && pytest
cd silentpulse-datasource && pytest
cd silentpulse-uc-discovery && pytest
```

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear, focused commits
3. Ensure linting passes and tests are green
4. Open a PR against `main` with a clear description
5. Wait for CI to pass and a maintainer review

## Subpackage Structure

Each subpackage is independent with its own `pyproject.toml`. When adding a new module:

- Place source code in the corresponding `silentpulse_<package>/` directory
- Add tests in the `tests/` directory within the subpackage
- Update the package's `__init__.py` exports if adding public API

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
