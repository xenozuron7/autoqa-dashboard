# Client Ticket Dashboard

A Flask-based dashboard that visualizes client ticket statistics (completed, processing, and failed) using MongoDB and Chart.js with Miniconda environment management.

## Features

- **Date-sensitive filtering**: Select any date to view ticket statistics
- **Separate bar graphs**: Completed, processing, failed and callback-needed tickets
- **Client-based filtering**: Select a client to show their ticket information.
- **Auto-refresh functionality**: Configurable automatic data updates
- **MongoDB integration**: Efficient data storage and retrieval
- **Real-time updates**: Charts update instantly when date changes
- **Miniconda environment**: Isolated Python environment for dependency management

## Prerequisites

- Miniconda or Anaconda installed
- MongoDB installed locally.
- Your dataset.(Can be MongoDB Atlas or locally provided dataset)

## Quick Start

### 1. Setup Environment

```bash
# Make setup script executable
chmod +x setup.sh

# Run setup
./setup.sh

#Make the run script executable
chmod +x run.sh

#Run the script
./run.sh
