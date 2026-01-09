# Client Ticket Dashboard

A Flask-based dashboard that visualizes client ticket statistics (completed, processing, and failed) using MongoDB and Chart.js with Miniconda environment management.

## Features

- **Date-sensitive filtering**: Select any date to view ticket statistics
- **Three separate bar graphs**: Completed, processing, and failed tickets
- **Auto-refresh functionality**: Configurable automatic data updates
- **MongoDB integration**: Efficient data storage and retrieval
- **Real-time updates**: Charts update instantly when date changes
- **Miniconda environment**: Isolated Python environment for dependency management

## Prerequisites

- Miniconda or Anaconda installed
- MongoDB installed locally (without root access)
- Your `clients_data.json` file

## Quick Start

### 1. Setup Environment

```bash
# Make setup script executable
chmod +x setup.sh

# Run setup
./setup.sh
