#!/bin/bash

MONGODB_HOME=$HOME/mongodb/mongodb-binaries

echo "Stopping MongoDB..."
$MONGODB_HOME/bin/mongosh --eval "db.adminCommand({shutdown: 1})" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "MongoDB stopped successfully."
else
    # Force kill if graceful shutdown fails
    pkill -f mongod
    echo "MongoDB process killed."
fi
