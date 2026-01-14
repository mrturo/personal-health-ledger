#!/bin/bash
# Quick setup script for Personal Health Ledger

set -e

echo "=== Personal Health Ledger - Setup Script ==="
echo ""

# Check Python version
echo "Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "Found Python $python_version"

if ! python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)"; then
    echo "ERROR: Python 3.10+ required"
    exit 1
fi

# Create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "Virtual environment created."
else
    echo ""
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt --quiet
pip install -e . --quiet

echo ""
echo "Installing development dependencies..."
pip install -r requirements-dev.txt --quiet

# Check credentials
echo ""
echo "Checking configuration..."

if [ ! -f "config/credentials.json" ] && [ ! -f "config/service_account.json" ]; then
    echo "WARNING: No credentials found in config/"
    echo "Please add either:"
    echo "  - config/credentials.json (OAuth2)"
    echo "  - config/service_account.json (Service Account)"
    echo ""
    echo "See config/README.md for instructions."
else
    echo "Credentials found ✓"
fi

# Run type checks
echo ""
echo "Running type checks..."
mypy src/ || echo "Type check warnings (review above)"

# Run tests
echo ""
echo "Running tests..."
pytest tests/ -q

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To activate the environment in your shell:"
echo "  source venv/bin/activate"
echo ""
echo "To run the CLI:"
echo "  phl --help"
echo "  phl all  # Run full pipeline"
echo ""
