# Client Ticket Dashboard

A FastAPI-based dashboard that visualizes client ticket statistics (completed, processing, failed, and callback) using MongoDB, Redis caching, and Chart.js - optimized for millions of records with Miniconda environment management.

## Features

- **Date-sensitive filtering**: Select any date to view ticket statistics
- **Multiple visualization modes**: Overview, client-specific, and date range analysis
- **Separate status tracking**: Completed, processing, failed and callback-required tickets
- **Client-based filtering**: Select a client to show their 7-day ticket trends
- **Auto-refresh functionality**: Configurable automatic data updates (5-300 seconds)
- **Smooth state persistence**: Remembers your tab, filters, and scroll position across reloads
- **Redis caching**: Materialized views for instant data retrieval (optional but recommended)
- **MongoDB streaming**: Efficiently handles millions of records without memory overflow
- **Real-time updates**: Charts and statistics update instantly when filters change
- **Miniconda environment**: Isolated Python environment for dependency management
- **Production-ready**: Optimized for large datasets with batch processing and parallel aggregation

## Prerequisites

- Miniconda or Anaconda installed
- MongoDB installed locally or MongoDB Atlas
- Redis installed (optional but recommended for performance)
- Your dataset (Can be MongoDB Atlas or locally provided dataset)

## Quick Start

```bash
# Make setup script executable
chmod +x setup.sh

# Run setup
./setup.sh

# Make the run script executable
chmod +x run.sh

# Run the script
./run.sh
```

## Manual Setup

### 1. Create Conda Environment

```bash
# Create environment
conda create -n dashboard python=3.12
conda activate dashboard

# Install dependencies
pip install fastapi uvicorn motor redis python-dotenv python-dateutil apscheduler
```

### 2. Configure Environment

Create a `.env` file in the project root:

```bash
# .env
MONGODB_URI=mongodb://localhost:27017
# Or for MongoDB Atlas:
# MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
```

### 3. Start Services

**Start Redis (recommended for performance):**
```bash
redis-server
```

**Start MongoDB (if using local instance):**
```bash
mongod
# Or with systemd:
sudo systemctl start mongod
```

### 4. Run Application

```bash
# Development mode
uvicorn app:app --host 0.0.0.0 --port 8000 --reload

# Production mode
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4
```

### 5. Access Dashboard

Open your browser: `http://localhost:8000`

## Project Structure

```
dashboard/
├── app.py                      # FastAPI backend (optimized for millions of records)
├── templates/
│   └── dashboard.html          # Frontend with smooth state transitions
├── static/
│   └── css/
│       └── style.css          # Responsive styling
├── .env                        # Environment configuration
├── setup.sh                    # Setup script
├── run.sh                      # Run script
└── README.md                   # This file
```

## Configuration

### Environment Variables

Edit `.env` file:

```bash
MONGODB_URI=mongodb://localhost:27017  # MongoDB connection string
```

### Performance Tuning

Edit constants in `app.py`:

```python
STREAM_BATCH_SIZE = 1000          # Documents per batch (increase for more memory)
DEFAULT_CACHE_TTL = 300           # Cache expiry in seconds (5 minutes)
MATERIALIZED_VIEW_TTL = 86400     # View cache expiry (24 hours)
PARALLEL_DATE_LIMIT = 5           # Concurrent date processing
```

## API Endpoints

### Core Endpoints

- `GET /` - Dashboard homepage
- `GET /api/ticket-data?date=YYYY-MM-DD` - Daily ticket statistics
- `GET /api/detailed-tickets?date=YYYY-MM-DD&status=completed` - Detailed ticket list
- `GET /api/client-tickets-7days?client_id=ABC` - 7-day client trend
- `GET /api/ticket-data-range?start_date=...&end_date=...` - Date range analysis
- `GET /api/all-clients?limit=1000&offset=0` - Paginated client list
- `GET /api/clients/search?q=ABC` - Client search

### Admin Endpoints

- `GET /api/refresh-view` - Trigger cache refresh (7 days)
- `GET /api/refresh-view-full` - Trigger full cache refresh (30 days)
- `GET /reload-data` - Clear all cache and refresh
- `GET /api/stats` - System statistics
- `GET /api/cache-metrics` - Cache hit rates

## Performance

**Optimized for millions of records:**

- **Overview load**: ~2-3 seconds (1M records)
- **With Redis cache**: ~50-100ms
- **Memory usage**: ~50MB (streaming mode)
- **Cache hit rate**: 85-95%

## Troubleshooting

### Redis Connection Failed

```
⚠️ Redis connection failed: Connection refused
⚠️ Application will work but without caching (slower)
```

**Solution:**
```bash
redis-server  # Start Redis
redis-cli ping  # Verify (should return PONG)
```

### MongoDB Connection Error

**Solution:**
```bash
# Check MongoDB is running
mongosh

# Or start MongoDB
sudo systemctl start mongod

# Verify connection string in .env
MONGODB_URI=mongodb://localhost:27017
```

### Slow Queries

The app creates indexes in the background. For large collections, wait 5-10 minutes or create manually:

```javascript
// In MongoDB shell
db.auto_qa_results_demo.createIndex(
  {created_at: -1, client_id: 1, auto_qa_status: 1},
  {background: true}
);
```

## Production Deployment

### Using Uvicorn

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4
```

### Using Gunicorn + Uvicorn

```bash
pip install gunicorn
gunicorn app:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Systemd Service

Create `/etc/systemd/system/dashboard.service`:

```ini
[Unit]
Description=Auto QA Dashboard
After=network.target mongodb.service redis.service

[Service]
Type=notify
User=www-data
WorkingDirectory=/path/to/dashboard
ExecStart=/path/to/venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable dashboard
sudo systemctl start dashboard
```

## Dashboard Features

### Overview Tab
- Daily ticket counts by status
- Interactive bar charts (click for details)
- Real-time statistics with animated counters

### Client Tab
- Client selector with search
- 7-day trend visualization
- Status breakdown by day

### Date Range Tab
- Multi-day analysis
- Client filtering (select multiple)
- Daily trend charts
- Client breakdown cards

## License

MIT License

## Author

Built for high-performance ticket analytics
