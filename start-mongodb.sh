#!/bin/bash

# Set OpenSSL library path if needed
export LD_LIBRARY_PATH=$HOME/mongodb/openssl/lib:$LD_LIBRARY_PATH

# Get absolute paths
MONGODB_HOME=$HOME/mongodb/mongodb-binaries
DATA_PATH=$HOME/mongodb/data
LOG_PATH=$HOME/mongodb/logs/mongod.log

echo "Starting MongoDB..."
echo "Data directory: $DATA_PATH"
echo "Log file: $LOG_PATH"

# Create directories if they don't exist
mkdir -p $DATA_PATH
mkdir -p $(dirname $LOG_PATH)

# Start MongoDB with explicit paths
$MONGODB_HOME/bin/mongod \
  --dbpath $DATA_PATH \
  --logpath $LOG_PATH \
  --port 27017 \
  --bind_ip 127.0.0.1 \
  --fork

if [ $? -eq 0 ]; then
    echo "MongoDB started successfully!"
    echo "Check logs at: $LOG_PATH"
else
    echo "Failed to start MongoDB. Check the log file for details."
    exit 1
fi
