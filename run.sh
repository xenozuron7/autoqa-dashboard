#!/bin/bash

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate ticket-dashboard

# Check if MongoDB is running
if ! pgrep -x "mongod" > /dev/null; then
    echo "Starting MongoDB..."
    ./start-mongodb.sh
    sleep 3
fi

# Check if clients_data.json exists
if [ ! -f "clients_data.json" ]; then
    echo "Error: clients_data.json not found!"
    echo "Please place your JSON file in the current directory."
    exit 1
fi

# Run Flask app
echo "Starting Flask dashboard..."
echo "Access the dashboard at: http://localhost:5000"
python app.py
