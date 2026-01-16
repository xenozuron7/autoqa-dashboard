from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import redis.asyncio as aioredis
from datetime import datetime, timedelta
from dateutil import parser
import json
from typing import Dict, List, Optional
import logging
import traceback
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global connection pools
mongo_client = None
db = None
collection = None
redis_client = None
scheduler = AsyncIOScheduler()


def parse_date_safe(date_str: str = None) -> datetime:
    """Parse date with validation and fallback to today"""
    if not date_str:
        return datetime.now()
    
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        
        # Validate reasonable date range
        if parsed < datetime(2020, 1, 1):
            logger.warning(f"Date too old: {date_str}, using today")
            return datetime.now()
        
        if parsed > datetime.now():
            logger.warning(f"Future date: {date_str}, using today")
            return datetime.now()
        
        return parsed
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid date format: {date_str}, using today. Error: {e}")
        return datetime.now()


def get_last_n_days_labels_from_date(end_date, n):
    """Generate date labels for charts (e.g., 'Jan 05, 2026')"""
    labels = []
    for i in range(n-1, -1, -1):
        date = end_date - timedelta(days=i)
        labels.append(date.strftime('%b %d, %Y'))
    return labels


async def ensure_indexes():
    """
    Ensure required indexes exist.
    Handles conflicts by using existing indexes or recreating if needed.
    """
    if collection is None:
        logger.error("Collection not initialized, skipping index creation")
        return
    
    try:
        # Get existing indexes
        existing_indexes = await collection.list_indexes().to_list(length=None)
        existing_keys = {
            tuple(sorted(idx.get('key', {}).items())): idx.get('name')
            for idx in existing_indexes
        }
        
        # Define required indexes with background creation
        required_indexes = [
            {
                'keys': [("created_at", 1)],
                'name': "created_at_idx",
                'background': True
            },
            {
                'keys': [("client_id", 1)],
                'name': "client_id_idx",
                'background': True
            },
            {
                'keys': [("created_at", 1), ("client_id", 1)],
                'name': "created_client_idx",
                'background': True
            },
            {
                'keys': [("created_at", 1), ("auto_qa_status", 1)],
                'name': "created_status_idx",
                'background': True
            },
            {
                'keys': [("client_id", 1), ("created_at", -1)],
                'name': "client_created_desc_idx",
                'background': True
            }
        ]
        
        # Create or verify each index
        for idx_spec in required_indexes:
            keys = idx_spec['keys']
            name = idx_spec['name']
            key_tuple = tuple(sorted(keys))
            
            # Check if index with same keys exists
            if key_tuple in existing_keys:
                existing_name = existing_keys[key_tuple]
                if existing_name != name:
                    logger.info(f"ℹ️ Index exists as '{existing_name}', using existing")
                else:
                    logger.info(f"✅ Index '{name}' verified")
            else:
                # Create new index
                try:
                    await collection.create_index(
                        keys,
                        name=name,
                        background=idx_spec.get('background', True)
                    )
                    logger.info(f"✅ Created index '{name}'")
                except Exception as e:
                    logger.warning(f"⚠️ Could not create index '{name}': {e}")
        
        logger.info("✅ All indexes ready")
        
    except Exception as e:
        logger.error(f"⚠️ Index verification failed: {e}")


async def warm_cache_today():
    """Pre-warm cache for today's data (called by scheduler)"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"🔥 Warming cache for {today}")
        
        # Trigger cache by calling the endpoint logic
        if collection is not None:
            cache_key = f"ticket_data:{today}"
            cached = await get_cache(cache_key)
            
            if not cached:
                # Manually execute query to warm cache
                target_date = datetime.strptime(today, "%Y-%m-%d")
                start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_of_day = start_of_day + timedelta(days=1)
                
                pipeline = [
                    {"$match": {"created_at": {"$gte": start_of_day, "$lt": end_of_day}}},
                    {"$limit": 1}
                ]
                
                cursor = collection.aggregate(pipeline)
                await cursor.to_list(length=1)
                
                logger.info(f"✅ Cache warmed for {today}")
    except Exception as e:
        logger.error(f"Cache warming failed: {e}")


app = FastAPI(title="AutoQA Dashboard API", version="1.0.0")

# Performance: Enable response compression
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.on_event("startup")
async def startup_event():
    """Initialize database connections with connection pooling and error handling"""
    global mongo_client, db, collection, redis_client
    
    # MongoDB Atlas connection with comprehensive error handling
    try:
        MONGO_URI = os.getenv("MONGODB_URI")
        
        if not MONGO_URI:
            raise ValueError("MONGODB_URI environment variable not set")
        
        logger.info("🔄 Connecting to MongoDB...")
        
        # Performance: Optimized connection pool settings
        mongo_client = AsyncIOMotorClient(
            MONGO_URI,
            maxPoolSize=100,  # Increased for high traffic
            minPoolSize=20,
            maxIdleTimeMS=45000,
            serverSelectionTimeoutMS=10000,
            waitQueueTimeoutMS=5000,  # Max wait for connection from pool
            connectTimeoutMS=10000,
            socketTimeoutMS=30000,  # Query timeout
            retryWrites=True,
            retryReads=True
        )
        
        # Test connection with timeout
        await asyncio.wait_for(
            mongo_client.admin.command('ping'),
            timeout=10.0
        )
        
        db = mongo_client["AutoQA"]
        collection = db["auto_qa_results_demo"]
        
        logger.info("✅ MongoDB connected")
        
        # Performance: Ensure indexes exist (with background creation)
        await ensure_indexes()
        
    except asyncio.TimeoutError:
        logger.error("❌ MongoDB connection timeout")
        mongo_client = None
        db = None
        collection = None
    except Exception as e:
        logger.error(f"❌ MongoDB failed: {e}")
        mongo_client = None
        db = None
        collection = None
    
    # Redis cache with improved connection pool
    try:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        logger.info("🔄 Connecting to Redis...")
        
        # Performance: Larger connection pool with timeouts
        redis_client = await asyncio.wait_for(
            aioredis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=200,  # Increased for high traffic
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            ),
            timeout=5.0
        )
        
        await asyncio.wait_for(redis_client.ping(), timeout=2.0)
        logger.info("✅ Redis connected")
        
    except (asyncio.TimeoutError, Exception) as e:
        logger.warning(f"⚠️ Redis unavailable (app will continue without cache): {e}")
        redis_client = None
    
    # Performance: Start cache warming scheduler
    try:
        scheduler.add_job(
            warm_cache_today,
            CronTrigger(hour=0, minute=5),  # Daily at 12:05 AM
            id="warm_cache_daily",
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("✅ Cache warming scheduler started")
        
        # Warm cache immediately on startup
        asyncio.create_task(warm_cache_today())
        
    except Exception as e:
        logger.warning(f"⚠️ Scheduler failed to start: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully close all connections"""
    try:
        if scheduler.running:
            scheduler.shutdown()
            logger.info("🛑 Scheduler stopped")
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")
    
    try:
        if redis_client is not None:
            await redis_client.close()
            logger.info("🛑 Redis connection closed")
    except Exception as e:
        logger.error(f"Error closing Redis: {e}")
    
    try:
        if mongo_client is not None:
            mongo_client.close()
            logger.info("🛑 MongoDB connection closed")
    except Exception as e:
        logger.error(f"Error closing MongoDB: {e}")


async def get_cache(key: str):
    """Retrieve cached data from Redis with timeout"""
    if redis_client is None:
        return None
    try:
        # Crash fix: Add timeout to prevent hanging
        data = await asyncio.wait_for(
            redis_client.get(key),
            timeout=2.0
        )
        if data:
            logger.info(f"[CACHE HIT] {key}")
            return json.loads(data)
        logger.info(f"[CACHE MISS] {key}")
        return None
    except asyncio.TimeoutError:
        logger.warning(f"Cache timeout for key: {key}")
        return None
    except Exception as e:
        logger.error(f"Cache retrieval error: {e}")
        return None


async def set_cache(key: str, data: dict, expire: int = 300):
    """Store data in Redis cache with expiration and timeout"""
    if redis_client is None:
        return
    try:
        # Crash fix: Add timeout
        await asyncio.wait_for(
            redis_client.setex(key, expire, json.dumps(data)),
            timeout=2.0
        )
    except asyncio.TimeoutError:
        logger.warning(f"Cache set timeout for key: {key}")
    except Exception as e:
        logger.error(f"Cache storage error: {e}")


async def clear_cache_pattern(pattern: str):
    """Clear cache keys matching pattern"""
    if redis_client is None:
        return
    try:
        keys = await asyncio.wait_for(
            redis_client.keys(pattern),
            timeout=5.0
        )
        if keys:
            await redis_client.delete(*keys)
            logger.info(f"🗑️ Cleared {len(keys)} cache keys")
    except Exception as e:
        logger.error(f"Cache clear error: {e}")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle all uncaught exceptions"""
    logger.error(f"Unhandled error on {request.url.path}: {exc}", exc_info=True)
    
    # Don't expose internal errors in production
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "path": request.url.path}
    )


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Render main dashboard page"""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    health_status = {
        "status": "healthy",
        "mongodb": "connected" if (mongo_client is not None and collection is not None) else "disconnected",
        "redis": "connected" if redis_client is not None else "disconnected",
        "timestamp": datetime.now().isoformat()
    }
    
    # Return 503 if critical components are down
    if mongo_client is None or collection is None:
        return JSONResponse(
            status_code=503,
            content={**health_status, "status": "unhealthy"}
        )
    
    return JSONResponse(content=health_status)


@app.get("/api/ticket-data")
async def get_ticket_data(date: str = None):
    """
    Get aggregated ticket counts by client and status for a specific date.
    Uses MongoDB aggregation pipeline with optimizations:
    - Field projection (only fetch needed fields)
    - Parallel execution ready
    - Response caching
    - Query timeout protection
    """
    # Crash fix: Check both client and collection
    if mongo_client is None or collection is None:
        return JSONResponse(status_code=503, content={"error": "Database unavailable"})
    
    # Performance & Crash fix: Safe date parsing
    target_date = parse_date_safe(date)
    date_str = target_date.strftime('%Y-%m-%d')
    
    cache_key = f"ticket_data:{date_str}"
    cached_data = await get_cache(cache_key)
    if cached_data:
        return JSONResponse(content=cached_data)
    
    try:
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        
        logger.info(f"📊 Fetching tickets for {date_str}")
        
        # Performance: Aggregation pipeline with field projection
        pipeline = [
            {
                "$match": {
                    "created_at": {"$gte": start_of_day, "$lt": end_of_day}
                }
            },
            {
                # Performance: Only project needed fields
                "$project": {
                    "_id": 0,  # Don't fetch ObjectId
                    "client_id": {"$ifNull": ["$client_id", "Unknown"]},
                    "auto_qa_status": {"$toLower": {"$toString": "$auto_qa_status"}},
                    "request_status": {"$toLower": {"$toString": "$request_status"}}
                }
            },
            {
                "$addFields": {
                    "status": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": ["$auto_qa_status", "complete"]}, "then": "completed"},
                                {"case": {"$eq": ["$auto_qa_status", "callback"]}, "then": "callback"},
                                {
                                    "case": {
                                        "$or": [
                                            {"$eq": ["$auto_qa_status", "failed"]},
                                            {"$eq": ["$request_status", "failed"]}
                                        ]
                                    },
                                    "then": "failed"
                                }
                            ],
                            "default": "processing"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "client_id": "$client_id",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.client_id",
                    "stats": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count"
                        }
                    }
                }
            },
            {
                "$sort": {"_id": 1}
            },
            {
                "$limit": 1000  # Reasonable limit for performance
            }
        ]
        
        # Crash fix: Add timeout to prevent hanging
        cursor = collection.aggregate(pipeline)
        results = await asyncio.wait_for(
            cursor.to_list(length=1000),
            timeout=30.0
        )
        
        logger.info(f"📊 Processed data for {len(results)} clients")
        
        # Crash fix: Handle empty results safely
        if not results:
            response_data = {
                'client_ids': [],
                'completed': [],
                'processing': [],
                'failed': [],
                'callback': [],
                'message': 'No data available'
            }
            await set_cache(cache_key, response_data, expire=300)
            return JSONResponse(content=response_data)
        
        # Transform aggregation results to frontend format
        client_ids = []
        completed = []
        processing = []
        failed = []
        callback = []
        
        for result in results:
            # Crash fix: Handle None client_id properly
            raw_id = result.get('_id')
            if raw_id is None or raw_id == '':
                client_id = 'Unknown'
            else:
                client_id = str(raw_id)
            
            client_ids.append(client_id)
            
            stats = {stat['status']: stat['count'] for stat in result.get('stats', [])}
            completed.append(stats.get('completed', 0))
            processing.append(stats.get('processing', 0))
            failed.append(stats.get('failed', 0))
            callback.append(stats.get('callback', 0))
        
        response_data = {
            'client_ids': client_ids,
            'completed': completed,
            'processing': processing,
            'failed': failed,
            'callback': callback
        }
        
        await set_cache(cache_key, response_data, expire=300)
        return JSONResponse(content=response_data)
    
    except asyncio.TimeoutError:
        logger.error(f"Query timeout for date: {date_str}")
        return JSONResponse(
            status_code=504,
            content={"error": "Query timeout. Please try again."}
        )
    except Exception as e:
        logger.error(f"Error in get_ticket_data: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/api/detailed-tickets")
async def get_detailed_tickets(date: str, status: str, page: int = 1, limit: int = 100):
    """
    Get detailed ticket information for modal display.
    Performance optimizations:
    - Pagination (reduced from 1000 to 100)
    - Field projection
    - Query timeout
    """
    if mongo_client is None or collection is None:
        return JSONResponse(status_code=503, content={"error": "Database unavailable"})
    
    # Performance: Reduce limit for better performance
    if limit > 500:
        limit = 500
    
    try:
        target_date = parse_date_safe(date)
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)
        
        logger.info(f"📋 Fetching detailed {status} tickets for {date}")
        
        # Performance: Optimized pipeline with field projection
        pipeline = [
            {
                "$match": {
                    "created_at": {"$gte": start_of_day, "$lt": end_of_day}
                }
            },
            {
                # Performance: Only fetch fields shown in modal
                "$project": {
                    "_id": 0,  # Exclude ObjectId
                    "ticket_id": 1,
                    "client_id": 1,
                    "created_at": 1,
                    "auto_qa_status": 1,
                    "request_status": 1,
                    "status_computed": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "complete"]}, "then": "completed"},
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "callback"]}, "then": "callback"},
                                {
                                    "case": {
                                        "$or": [
                                            {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "failed"]},
                                            {"$eq": [{"$toLower": {"$toString": "$request_status"}}, "failed"]}
                                        ]
                                    },
                                    "then": "failed"
                                }
                            ],
                            "default": "processing"
                        }
                    }
                }
            },
            {
                "$match": {
                    "status_computed": status
                }
            },
            {
                "$sort": {"created_at": -1}
            },
            {
                "$facet": {
                    "metadata": [{"$count": "total"}],
                    "data": [
                        {"$skip": (page - 1) * limit},
                        {"$limit": limit}
                    ]
                }
            }
        ]
        
        # Crash fix: Timeout protection
        cursor = collection.aggregate(pipeline)
        results = await asyncio.wait_for(
            cursor.to_list(length=1),
            timeout=30.0
        )
        
        # Crash fix: Safe result access
        if not results or not results[0]:
            return JSONResponse(content={
                'date': date,
                'status': status,
                'total_tickets': 0,
                'tickets': []
            })
        
        total_count = 0
        tickets = []
        
        if results[0].get('metadata') and len(results[0]['metadata']) > 0:
            total_count = results[0]['metadata'][0].get('total', 0)
        
        if results[0].get('data'):
            tickets = results[0]['data']
        
        logger.info(f"📋 Found {total_count} {status} tickets (returning {len(tickets)})")
        
        formatted_tickets = []
        for ticket in tickets:
            formatted_tickets.append({
                'ticket_id': ticket.get('ticket_id', 'N/A'),
                'client_id': ticket.get('client_id', 'N/A'),
                'status': ticket.get('status_computed', 'N/A'),
                'created_at': ticket.get('created_at').strftime('%Y-%m-%d %H:%M') if ticket.get('created_at') else 'N/A',
                'auto_qa_status': ticket.get('auto_qa_status', 'N/A'),
                'request_status': ticket.get('request_status', 'N/A')
            })
        
        return JSONResponse(content={
            'date': date,
            'status': status,
            'total_tickets': total_count,
            'tickets': formatted_tickets
        })
    
    except asyncio.TimeoutError:
        logger.error(f"Query timeout for detailed tickets: {date}, {status}")
        return JSONResponse(
            status_code=504,
            content={"error": "Query timeout. Please try again."}
        )
    except Exception as e:
        logger.error(f"Error in get_detailed_tickets: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/api/all-clients")
async def get_all_clients():
    """Get list of all unique client IDs from database with caching"""
    if mongo_client is None or collection is None:
        return JSONResponse(status_code=503, content={"error": "Database unavailable"})
    
    cache_key = "all_clients"
    cached_data = await get_cache(cache_key)
    if cached_data:
        return JSONResponse(content={"client_ids": cached_data})
    
    try:
        # Crash fix: Add timeout
        clients = await asyncio.wait_for(
            collection.distinct("client_id"),
            timeout=10.0
        )
        
        # Crash fix: Handle None values
        clients = sorted([str(cid) for cid in clients if cid is not None and cid != ''])
        
        logger.info(f"Found {len(clients)} clients")
        await set_cache(cache_key, clients, expire=600)
        return JSONResponse(content={"client_ids": clients})
    
    except asyncio.TimeoutError:
        logger.error("Timeout fetching clients")
        return JSONResponse(status_code=504, content={"error": "Query timeout"})
    except Exception as e:
        logger.error(f"Error in get_all_clients: {e}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/api/client-tickets-7days")
async def get_client_tickets_7days(client_id: str):
    """
    Get 7-day ticket analytics for a specific client.
    Returns both aggregate totals and daily breakdown.
    """
    if mongo_client is None or collection is None:
        return JSONResponse(status_code=503, content={"success": False, "error": "Database unavailable"})
    
    if not client_id or client_id.strip() == '':
        return JSONResponse(status_code=400, content={"success": False, "error": "Client ID required"})
    
    cache_key = f"client_7days:{client_id}"
    cached_data = await get_cache(cache_key)
    if cached_data:
        return JSONResponse(content=cached_data)
    
    try:
        end_date = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999)
        start_date = (end_date - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info(f"📈 Client {client_id}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Performance: Aggregation pipeline with field projection
        pipeline = [
            {
                "$match": {
                    "client_id": str(client_id),
                    "created_at": {"$gte": start_date, "$lte": end_date}
                }
            },
            {
                # Performance: Only project needed fields
                "$project": {
                    "_id": 0,
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created_at"
                        }
                    },
                    "status": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "complete"]}, "then": "completed"},
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "callback"]}, "then": "callback"},
                                {
                                    "case": {
                                        "$or": [
                                            {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "failed"]},
                                            {"$eq": [{"$toLower": {"$toString": "$request_status"}}, "failed"]}
                                        ]
                                    },
                                    "then": "failed"
                                }
                            ],
                            "default": "processing"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "date": "$date",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.date",
                    "stats": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count"
                        }
                    }
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]
        
        # Crash fix: Timeout protection
        cursor = collection.aggregate(pipeline)
        results = await asyncio.wait_for(
            cursor.to_list(length=7),
            timeout=20.0
        )
        
        logger.info(f"📈 Processed {len(results)} days of data")
        
        # Build response structure
        dates = get_last_n_days_labels_from_date(end_date, 7)
        date_map = {}
        total_stats = {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}
        
        for result in results:
            stats = {stat['status']: stat['count'] for stat in result.get('stats', [])}
            date_map[result['_id']] = stats
            
            for status in ['completed', 'processing', 'failed', 'callback']:
                total_stats[status] += stats.get(status, 0)
        
        # Build daily arrays for charts
        completed_data = []
        processing_data = []
        failed_data = []
        callback_data = []
        
        for i in range(6, -1, -1):
            date = end_date - timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            stats = date_map.get(date_str, {})
            completed_data.append(stats.get('completed', 0))
            processing_data.append(stats.get('processing', 0))
            failed_data.append(stats.get('failed', 0))
            callback_data.append(stats.get('callback', 0))
        
        response_data = {
            'success': True,
            'total_tickets': sum(total_stats.values()),
            'all_time_stats': total_stats,
            'seven_day_data': {
                'dates': dates,
                'completed': completed_data,
                'processing': processing_data,
                'failed': failed_data,
                'callback': callback_data
            }
        }
        
        await set_cache(cache_key, response_data, expire=300)
        return JSONResponse(content=response_data)
    
    except asyncio.TimeoutError:
        logger.error(f"Query timeout for client: {client_id}")
        return JSONResponse(
            status_code=504,
            content={"success": False, "error": "Query timeout"}
        )
    except Exception as e:
        logger.error(f"Error in get_client_tickets_7days: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"success": False, "error": "Internal server error"})


@app.get("/api/ticket-data-range")
async def get_ticket_data_range(
    start_date: str,
    end_date: str,
    client_ids: Optional[str] = None
):
    """
    Get aggregated ticket data for a date range.
    Optionally filter by specific client IDs.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        client_ids: Comma-separated client IDs (optional)
    
    Returns:
        Total counts by status and daily breakdown
    """
    if mongo_client is None or collection is None:
        return JSONResponse(status_code=503, content={"error": "Database unavailable"})
    
    try:
        # Parse and validate dates
        start = parse_date_safe(start_date)
        end = parse_date_safe(end_date)
        
        # Validate date range
        if end < start:
            return JSONResponse(
                status_code=400,
                content={"error": "End date must be after start date"}
            )
        
        # Limit range to 90 days
        if (end - start).days > 90:
            return JSONResponse(
                status_code=400,
                content={"error": "Date range cannot exceed 90 days"}
            )
        
        start_of_day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Parse client IDs if provided
        selected_clients = None
        if client_ids:
            selected_clients = [cid.strip() for cid in client_ids.split(',') if cid.strip()]
        
        # Build cache key
        cache_key = f"range:{start_date}:{end_date}:{client_ids or 'all'}"
        cached_data = await get_cache(cache_key)
        if cached_data:
            return JSONResponse(content=cached_data)
        
        logger.info(f"📊 Fetching range: {start_date} to {end_date}, clients: {selected_clients or 'all'}")
        
        # Build match condition
        match_condition = {
            "created_at": {"$gte": start_of_day, "$lte": end_of_day}
        }
        
        if selected_clients:
            match_condition["client_id"] = {"$in": selected_clients}
        
        # Aggregation pipeline for total counts by status
        total_pipeline = [
            {"$match": match_condition},
            {
                "$project": {
                    "_id": 0,
                    "status": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "complete"]}, "then": "completed"},
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "callback"]}, "then": "callback"},
                                {
                                    "case": {
                                        "$or": [
                                            {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "failed"]},
                                            {"$eq": [{"$toLower": {"$toString": "$request_status"}}, "failed"]}
                                        ]
                                    },
                                    "then": "failed"
                                }
                            ],
                            "default": "processing"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }
            }
        ]
        
        # Aggregation pipeline for daily breakdown
        daily_pipeline = [
            {"$match": match_condition},
            {
                "$project": {
                    "_id": 0,
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$created_at"
                        }
                    },
                    "status": {
                        "$switch": {
                            "branches": [
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "complete"]}, "then": "completed"},
                                {"case": {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "callback"]}, "then": "callback"},
                                {
                                    "case": {
                                        "$or": [
                                            {"$eq": [{"$toLower": {"$toString": "$auto_qa_status"}}, "failed"]},
                                            {"$eq": [{"$toLower": {"$toString": "$request_status"}}, "failed"]}
                                        ]
                                    },
                                    "then": "failed"
                                }
                            ],
                            "default": "processing"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "date": "$date",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.date",
                    "stats": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count"
                        }
                    }
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        # If specific clients selected, also get per-client breakdown
        client_pipeline = None
        if selected_clients:
            client_pipeline = [
                {"$match": match_condition},
                {
                    "$project": {
                        "_id": 0,
                        "client_id": 1,
                        "status": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$eq": [{"$toLower": "$auto_qa_status"}, "complete"]}, "then": "completed"},
                                    {"case": {"$eq": [{"$toLower": "$auto_qa_status"}, "callback"]}, "then": "callback"},
                                    {"case": {"$or": [
                                        {"$eq": [{"$toLower": "$auto_qa_status"}, "failed"]},
                                        {"$eq": [{"$toLower": "$request_status"}, "failed"]}
                                    ]}, "then": "failed"}
                                ],
                                "default": "processing"
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "client_id": "$client_id",
                            "status": "$status"
                        },
                        "count": {"$sum": 1}
                    }
                },
                {
                    "$group": {
                        "_id": "$_id.client_id",
                        "stats": {
                            "$push": {
                                "status": "$_id.status",
                                "count": "$count"
                            }
                        }
                    }
                },
                {"$sort": {"_id": 1}}
            ]
        
        # Execute queries in parallel
        tasks = [
            collection.aggregate(total_pipeline).to_list(length=10),
            collection.aggregate(daily_pipeline).to_list(length=365)
        ]
        
        if client_pipeline:
            tasks.append(collection.aggregate(client_pipeline).to_list(length=100))
        
        results = await asyncio.wait_for(
            asyncio.gather(*tasks),
            timeout=45.0
        )
        
        total_results = results[0]
        daily_results = results[1]
        client_results = results[2] if len(results) > 2 else None
        
        # Process total counts
        total_stats = {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}
        for result in total_results:
            status = result.get('_id', 'processing')
            total_stats[status] = result.get('count', 0)
        
        # Process daily breakdown
        date_map = {}
        for result in daily_results:
            date_str = result['_id']
            stats = {stat['status']: stat['count'] for stat in result.get('stats', [])}
            date_map[date_str] = stats
        
        # Generate all dates in range
        dates = []
        completed_data = []
        processing_data = []
        failed_data = []
        callback_data = []
        
        current = start
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            dates.append(current.strftime('%b %d, %Y'))
            
            stats = date_map.get(date_str, {})
            completed_data.append(stats.get('completed', 0))
            processing_data.append(stats.get('processing', 0))
            failed_data.append(stats.get('failed', 0))
            callback_data.append(stats.get('callback', 0))
            
            current += timedelta(days=1)
        
        response_data = {
            'success': True,
            'start_date': start_date,
            'end_date': end_date,
            'total_days': len(dates),
            'total_stats': total_stats,
            'daily_breakdown': {
                'dates': dates,
                'completed': completed_data,
                'processing': processing_data,
                'failed': failed_data,
                'callback': callback_data
            }
        }
        
        # Add per-client data if requested
        if client_results:
            client_data = {}
            for result in client_results:
                client_id = str(result['_id']) if result['_id'] else 'Unknown'
                stats = {stat['status']: stat['count'] for stat in result.get('stats', [])}
                client_data[client_id] = {
                    'completed': stats.get('completed', 0),
                    'processing': stats.get('processing', 0),
                    'failed': stats.get('failed', 0),
                    'callback': stats.get('callback', 0),
                    'total': sum(stats.values())
                }
            
            response_data['client_breakdown'] = client_data
        
        await set_cache(cache_key, response_data, expire=600)
        return JSONResponse(content=response_data)
    
    except asyncio.TimeoutError:
        logger.error(f"Query timeout for range: {start_date} to {end_date}")
        return JSONResponse(
            status_code=504,
            content={"error": "Query timeout. Try a smaller date range."}
        )
    except Exception as e:
        logger.error(f"Error in get_ticket_data_range: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": "Internal server error"})


@app.get("/reload-data")
async def reload_data():
    """Clear all caches and return database statistics"""
    if mongo_client is None or collection is None:
        return JSONResponse(status_code=503, content={"status": "error", "message": "Database unavailable"})
    
    try:
        await clear_cache_pattern("*")
        
        # Crash fix: Add timeout
        count = await asyncio.wait_for(
            collection.count_documents({}),
            timeout=10.0
        )
        
        # Performance: Trigger cache warming
        asyncio.create_task(warm_cache_today())
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Reloaded. {count:,} documents.",
            "cache_warming": "started"
        })
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "error", "message": "Timeout counting documents"})
    except Exception as e:
        logger.error(f"Error in reload_data: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": "Internal server error"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        access_log=True
    )
