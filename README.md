# Auto QA Analytics Dashboard

A modern, real-time analytics dashboard for tracking ticket status and token usage. Built with FastAPI, MongoDB, and Chart.js.

![Dashboard Preview](https://img.shields.io/badge/Status-Active-success)

## ✨ Features

- 📊 **Real-time Analytics** - Track completed, processing, failed, and callback tickets
- 👥 **Client Insights** - View individual client statistics and trends
- 📅 **Date Range Analysis** - Analyze data across custom date ranges
- 💰 **Token Analytics** - Monitor LLM token usage and costs by model
- ⚡ **Fast Performance** - Redis caching handles millions of records
- 🎨 **Modern UI** - Clean, responsive design with smooth animations

## 🚀 Quick Start

### 1. Clone & Setup

```bash
git clone https://github.com/xenozuron7/autoqa-dashboard.git
cd autoqa-dashboard

# Create conda environment
conda create -n dashboard python=3.12
conda activate dashboard

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure

Create a `.env` file:

```bash
MONGODB_URI=mongodb://localhost:27017
```

### 3. Run

```bash
# Start Redis (optional, but recommended)
redis-server

# Run the app
python app.py
```

Open **http://localhost:5000** in your browser 🎉

## 📁 Project Structure

```
dashboard/
├── app.py              # Backend API (FastAPI)
├── templates/
│   └── dashboard.html  # Frontend UI
├── static/css/
│   └── style.css       # Styling
├── .env                # Configuration
└── README.md
```

## 🔧 Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017` |

## 📖 API Reference

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard UI |
| `GET /api/overview?date=YYYY-MM-DD` | Daily statistics |
| `GET /api/client/{id}` | Client details |
| `GET /api/ticket-data-range?start_date=...&end_date=...` | Date range data |
| `GET /api/token-analytics?start_date=...&end_date=...` | Token usage stats |

## 🛠️ Tech Stack

- **Backend**: FastAPI, Motor (async MongoDB), Redis
- **Frontend**: HTML, CSS, JavaScript, Chart.js
- **Database**: MongoDB
- **Caching**: Redis

## 📝 License

MIT License
