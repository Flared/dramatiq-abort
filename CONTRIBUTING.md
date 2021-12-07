All contributions are welcomed, but we reserve the right to refuse additionnal
features to keep this project as lightweight and simple as possible.

# Development Guide

To install the development environment simply:

```
python -m venv venv
pip install .[dev]
```

You can now:

 * Test: `pytest`
 * Format code: `black . && isort .`
 * Lint: `flake8 .`
 * Check types: `mypy .`
 * Build doc: `make -C docs html`

Or do it all with:

```
tox
```

Successful `tox` run is a prerequisite for a Pull Request to be merged.

# Release Guide

Simply create a Github release. A Github action is defined on `.github/workflows/publish.yaml`.
