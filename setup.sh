#!/bin/bash

echo "==================================="
echo "  Ticket Dashboard Setup Script"
echo "==================================="
echo ""

# Check if conda is installed
if ! command -v conda &> /dev/null; then
    echo "ERROR: Conda not found!"
    echo "Please install Miniconda first from: https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi

# Create conda environment
echo "Creating conda environment 'ticket-dashboard'..."
conda create -n ticket-dashboard python=3.10 -y

# Activate environment
echo "Activating environment..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate ticket-dashboard

# Install packages
echo "Installing Flask..."
conda install flask -y

echo "Installing PyMongo..."
pip install pymongo

# Verify installation
echo ""
echo "Verifying installation..."
python -c "import flask; import pymongo; print('✓ All packages installed successfully!')"

# Make scripts executable
chmod +x start-mongodb.sh
chmod +x stop-mongodb.sh
chmod +x run.sh

echo ""
echo "==================================="
echo "  Setup Complete!"
echo "==================================="
echo ""
echo "Next steps:"
echo "1. Place your clients_data.json in this directory"
echo "2. Start MongoDB: ./start-mongodb.sh"
echo "3. Run the dashboard: ./run.sh"
echo "4. Open browser: http://localhost:5000"
echo ""
