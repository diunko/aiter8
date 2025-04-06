.PHONY: clean install-dev build test upload-test upload venv

# Variables
PACKAGE_NAME = iter8
PYTHON = python3
PIP = pip
VENV = .pyenv
VENV_ACTIVATE = $(VENV)/bin/activate
VENV_RUN = . $(VENV_ACTIVATE) &&

# Default target
all: clean build

# Create virtualenv
venv:
	$(PYTHON) -m venv $(VENV)
	$(VENV_RUN) $(PIP) install --upgrade pip setuptools wheel

# Install development dependencies
install-dev: venv
	$(VENV_RUN) $(PIP) install -e ".[dev]"
	@echo "Installed $(PACKAGE_NAME) in development mode with dev dependencies"

# Install just the package
install: venv
	$(VENV_RUN) $(PIP) install -e .
	@echo "Installed $(PACKAGE_NAME) in development mode"

# Run tests
test: install-dev
	$(VENV_RUN) pytest -xvs tests/
	@echo "All tests passed!"

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	@echo "Cleaned up build artifacts"

# Build package
build: clean
	$(VENV_RUN) $(PYTHON) -m build
	@echo "Built distribution packages in ./dist/"

# Upload to TestPyPI
upload-test: build
	$(VENV_RUN) $(PYTHON) -m twine upload --repository testpypi dist/*
	@echo "Uploaded to TestPyPI"
	@echo "You can install with: pip install --index-url https://test.pypi.org/simple/ $(PACKAGE_NAME)"

# Upload to PyPI
upload: build
	@echo "About to upload to PyPI. This is irreversible. Continue? [y/N] " && read ans && [ $${ans:-N} = y ]
	$(VENV_RUN) $(PYTHON) -m twine upload dist/*
	@echo "Uploaded to PyPI"
	@echo "You can install with: pip install $(PACKAGE_NAME)"

# Show help
help:
	@echo "Available targets:"
	@echo "  make install-dev    - Create venv and install package in dev mode with all dev dependencies"
	@echo "  make install        - Create venv and install package in dev mode"
	@echo "  make build          - Build distribution packages"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make test           - Run tests"
	@echo "  make upload-test    - Upload package to TestPyPI"
	@echo "  make upload         - Upload package to PyPI (prompts for confirmation)"
	@echo "  make help           - Show this help message" 