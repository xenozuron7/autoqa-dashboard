from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.middleware.gzip import GZipMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReadPreference
import redis.asyncio as aioredis
from datetime import datetime, timedelta
from dateutil import parser
import json
import time
from typing import Dict, List, Optional, Callable, Any, AsyncIterator
import logging
import traceback
import asyncio
from functools import wraps
from collections import defaultdict
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import hashlib

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

# Constants for MILLIONS of records
MAIN_COLLECTION_NAME = "auto_qa_results_demo"
DEFAULT_CACHE_TTL = 300
VIEW_CACHE_TTL = 1800
CLIENT_LIST_CACHE_TTL = 7200
INCREMENTAL_REFRESH_DAYS = 7

# Streaming and batching - critical for millions of records
STREAM_BATCH_SIZE = 1000          # Process 1k docs at a time
MAX_MEMORY_BATCH = 5000           # Max docs in memory at once
CURSOR_BATCH_SIZE = 500           # MongoDB cursor batch size
PARALLEL_DATE_LIMIT = 5           # Max parallel date processing
AGGREGATION_TIMEOUT_MS = 30000    # 30 second timeout

# Redis configuration for millions
REDIS_PIPELINE_SIZE = 100         # Batch Redis operations
MATERIALIZED_VIEW_PREFIX = "matview:"
MATERIALIZED_VIEW_TTL = 86400

# Smart limits based on data volume
SMALL_CLIENT_THRESHOLD = 1000     # Clients with <1k tickets
LARGE_CLIENT_THRESHOLD = 100000   # Clients with >100k tickets

# Rate limiting
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60
rate_limit_storage = defaultdict(list)

# Cache metrics
cache_metrics = {
    "hits": 0, "misses": 0, "total_requests": 0, "hit_rate": 0.0,
    "by_endpoint": defaultdict(lambda: {"hits": 0, "misses": 0})
}

# Memory monitoring
memory_stats = {
    "peak_batch_size": 0,
    "total_processed": 0,
    "streaming_queries": 0
}

# ============================================================================
# RATE LIMITING
# ============================================================================

async def check_rate_limit(request: Request) -> bool:
    """Check if request exceeds rate limit"""
    client_ip = request.client.host
    current_time = time.time()
    
    rate_limit_storage[client_ip] = [
        timestamp for timestamp in rate_limit_storage[client_ip]
        if current_time - timestamp < RATE_LIMIT_WINDOW
    ]
    
    if len(rate_limit_storage[client_ip]) >= RATE_LIMIT_REQUESTS:
        return False
    
    rate_limit_storage[client_ip].append(current_time)
    return True

def rate_limit(func: Callable):
    """Rate limiting decorator"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        request = kwargs.get('request') or (args[0] if args and isinstance(args[0], Request) else None)
        if request and not await check_rate_limit(request):
            return JSONResponse(status_code=429, content={
                "error": "Rate limit exceeded",
                "message": f"Maximum {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds"
            })
        return await func(*args, **kwargs)
    return wrapper

# ============================================================================
# DATA NORMALIZATION (Lightweight for streaming)
# ============================================================================

def normalize_ticket_document(raw_doc: dict) -> dict:
    """Lightweight normalization for streaming - minimal processing"""
    try:
        created_at = raw_doc.get('created_at')
        if not isinstance(created_at, datetime):
            if isinstance(created_at, str):
                created_at = parser.parse(created_at)
            elif isinstance(created_at, dict) and '$date' in created_at:
                created_at = parser.parse(created_at['$date'])
            else:
                created_at = datetime.now()
        
        client_id = raw_doc.get('client_id') or 'unknown'
        if not isinstance(client_id, str):
            client_id = str(client_id)
        
        auto_qa_status = str(raw_doc.get('auto_qa_status', '')).lower()
        request_status = str(raw_doc.get('request_status', '')).lower()
        ticket_id = raw_doc.get('ticket_id') or raw_doc.get('_id') or 'unknown'
        
        return {
            'ticket_id': str(ticket_id),
            'client_id': client_id,
            'created_at': created_at,
            'auto_qa_status': auto_qa_status,
            'request_status': request_status,
            'normalized': True
        }
    except Exception as e:
        logger.error(f"Normalization error: {e}")
        return {
            'ticket_id': 'unknown',
            'client_id': 'unknown',
            'created_at': datetime.now(),
            'auto_qa_status': '',
            'request_status': '',
            'normalized': False
        }

def parse_ticket_status(ticket: dict) -> str:
    """Fast status parsing"""
    auto_qa_status = ticket.get('auto_qa_status', '')
    request_status = ticket.get('request_status', '')
    
    if auto_qa_status == 'complete':
        return 'completed'
    elif auto_qa_status == 'callback':
        return 'callback'
    elif auto_qa_status == 'failed' or request_status == 'failed':
        return 'failed'
    return 'processing'

def extract_date(ticket: dict) -> datetime:
    """Fast date extraction"""
    date_obj = ticket.get('created_at')
    
    if isinstance(date_obj, datetime):
        return date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

def init_stats_dict() -> dict:
    """Initialize empty stats dictionary"""
    return {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}

# ============================================================================
# STATUS MATCH CONDITIONS (Push filtering to MongoDB)
# ============================================================================

def build_status_match_conditions(status: str) -> dict:
    """
    Build MongoDB query to filter by status at database level
    This avoids loading unnecessary documents into memory
    """
    status_lower = status.lower()
    
    if status_lower == 'completed':
        return {"auto_qa_status": {"$regex": "^complete$", "$options": "i"}}
    elif status_lower == 'callback':
        return {"auto_qa_status": {"$regex": "^callback$", "$options": "i"}}
    elif status_lower == 'failed':
        return {
            "$or": [
                {"auto_qa_status": {"$regex": "^failed$", "$options": "i"}},
                {"request_status": {"$regex": "^failed$", "$options": "i"}}
            ]
        }
    elif status_lower == 'processing':
        return {
            "auto_qa_status": {
                "$not": {"$regex": "^(complete|callback|failed)$", "$options": "i"}
            },
            "$or": [
                {"request_status": {"$exists": False}},
                {"request_status": {"$not": {"$regex": "^failed$", "$options": "i"}}}
            ]
        }
    
    return {}

# ============================================================================
# STREAMING DATABASE OPERATIONS (for MILLIONS of records)
# ============================================================================

async def stream_tickets(
    match_conditions: dict,
    projection: dict,
    batch_size: int = STREAM_BATCH_SIZE,
    skip: int = 0,
    sort: List[tuple] = None
) -> AsyncIterator[List[dict]]:
    """
    STREAM tickets in batches - never loads all into memory
    Critical for millions of records
    """
    try:
        cursor = collection.find(match_conditions, projection)
        
        if sort:
            cursor = cursor.sort(sort)
        
        cursor = cursor.batch_size(CURSOR_BATCH_SIZE)
        
        if skip > 0:
            cursor = cursor.skip(skip)
        
        batch = []
        async for document in cursor:
            batch.append(document)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
                memory_stats["streaming_queries"] += 1
        
        if batch:
            yield batch
            
        logger.info(f"Streamed documents with batch_size={batch_size}")
        
    except Exception as e:
        logger.error(f"Streaming error: {e}")
        yield []

async def stream_tickets_with_limit(
    match_conditions: dict,
    projection: dict,
    max_results: int = 1000,
    sort: List[tuple] = None
) -> AsyncIterator[dict]:
    """
    Stream tickets but yield one at a time, stop at max_results
    Memory efficient - no list building
    """
    try:
        cursor = collection.find(match_conditions, projection)
        
        if sort:
            cursor = cursor.sort(sort)
        
        cursor = cursor.batch_size(CURSOR_BATCH_SIZE).limit(max_results)
        
        count = 0
        async for document in cursor:
            yield document
            count += 1
            
            if count >= max_results:
                break
        
        logger.info(f"Streamed {count} documents (limit={max_results})")
        
    except Exception as e:
        logger.error(f"Streaming error: {e}")

async def count_tickets_estimate(match_conditions: dict = None) -> int:
    """
    FAST count using estimated_document_count or aggregation
    Avoids slow count_documents on millions of records
    """
    try:
        if not match_conditions or match_conditions == {}:
            count = await collection.estimated_document_count()
            return count
        else:
            pipeline = [
                {"$match": match_conditions},
                {"$count": "total"}
            ]
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            result = await cursor.to_list(length=1)
            return result[0]["total"] if result else 0
            
    except Exception as e:
        logger.error(f"Count error: {e}")
        return 0

async def fetch_tickets_cursor_limited(
    match_conditions: dict,
    projection: dict,
    limit: int = 10000,
    sort: List[tuple] = None,
    hint_index: str = None
) -> List[dict]:
    """
    Fetch with hard limits - prevents memory overflow
    For millions: NEVER fetch without limit
    """
    try:
        safe_limit = min(limit, 50000)
        
        cursor = collection.find(match_conditions, projection)
        
        if hint_index:
            cursor = cursor.hint(hint_index)
        
        cursor = cursor.batch_size(CURSOR_BATCH_SIZE)
        
        if sort:
            cursor = cursor.sort(sort)
        
        cursor = cursor.limit(safe_limit)
        
        tickets = await cursor.to_list(length=safe_limit)
        
        logger.info(f"Fetched {len(tickets)} tickets (limit={safe_limit})")
        return tickets
        
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return []

# ============================================================================
# STREAMING AGGREGATION (Process millions without loading all)
# ============================================================================

async def streaming_aggregate_by_client_status(
    match_conditions: dict,
    batch_size: int = STREAM_BATCH_SIZE
) -> Dict[str, Dict[str, int]]:
    """
    Stream-based aggregation for MILLIONS of records
    Processes in batches, never loads all into memory
    """
    client_stats = {}
    total_processed = 0
    
    projection = {
        "client_id": 1,
        "auto_qa_status": 1,
        "request_status": 1,
        "_id": 0
    }
    
    try:
        async for batch in stream_tickets(match_conditions, projection, batch_size):
            for ticket in batch:
                normalized = normalize_ticket_document(ticket)
                client_id = normalized.get('client_id', 'unknown')
                status = parse_ticket_status(normalized)
                
                if client_id not in client_stats:
                    client_stats[client_id] = init_stats_dict()
                
                if status in client_stats[client_id]:
                    client_stats[client_id][status] += 1
            
            total_processed += len(batch)
            
            if total_processed % 50000 == 0:
                logger.info(f"Processed {total_processed:,} tickets, {len(client_stats):,} unique clients")
        
        memory_stats["peak_batch_size"] = max(memory_stats["peak_batch_size"], total_processed)
        logger.info(f"Completed streaming aggregation: {total_processed:,} tickets, {len(client_stats):,} clients")
        
        return client_stats
        
    except Exception as e:
        logger.error(f"Streaming aggregation error: {e}")
        return client_stats

async def streaming_aggregate_by_date_status(
    match_conditions: dict,
    start_date: datetime,
    days: int,
    batch_size: int = STREAM_BATCH_SIZE
) -> Dict[str, Dict[str, int]]:
    """
    Stream-based date aggregation for MILLIONS
    """
    daily_counts = {}
    
    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        daily_counts[date_str] = init_stats_dict()
    
    projection = {
        "created_at": 1,
        "auto_qa_status": 1,
        "request_status": 1,
        "_id": 0
    }
    
    total_processed = 0
    
    try:
        async for batch in stream_tickets(match_conditions, projection, batch_size):
            for ticket in batch:
                normalized = normalize_ticket_document(ticket)
                date_str = extract_date(normalized).strftime('%Y-%m-%d')
                
                if date_str in daily_counts:
                    status = parse_ticket_status(normalized)
                    if status in daily_counts[date_str]:
                        daily_counts[date_str][status] += 1
            
            total_processed += len(batch)
            
            if total_processed % 50000 == 0:
                logger.info(f"Date aggregation: {total_processed:,} tickets processed")
        
        logger.info(f"Completed date aggregation: {total_processed:,} tickets")
        return daily_counts
        
    except Exception as e:
        logger.error(f"Date aggregation error: {e}")
        return daily_counts

# ============================================================================
# PARALLEL PROCESSING (for multiple date ranges)
# ============================================================================

async def parallel_aggregate_dates(dates: List[str], semaphore_limit: int = PARALLEL_DATE_LIMIT) -> Dict[str, Dict]:
    """
    Process multiple dates in parallel with concurrency control
    Prevents overwhelming MongoDB with millions of concurrent queries
    """
    semaphore = asyncio.Semaphore(semaphore_limit)
    
    async def process_single_date(date_str: str):
        async with semaphore:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d")
                start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end = target_date.replace(hour=23, minute=59, second=59, microsecond=999)
                
                match_conditions = {"created_at": {"$gte": start, "$lte": end}}
                
                client_stats = await streaming_aggregate_by_client_status(match_conditions)
                
                logger.info(f"Completed aggregation for {date_str}: {len(client_stats)} clients")
                return date_str, client_stats
                
            except Exception as e:
                logger.error(f"Error processing {date_str}: {e}")
                return date_str, {}
    
    tasks = [process_single_date(date) for date in dates]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    date_results = {}
    for result in results:
        if isinstance(result, tuple) and len(result) == 2:
            date_str, stats = result
            date_results[date_str] = stats
    
    return date_results

# ============================================================================
# REDIS BULK OPERATIONS (for millions of cache entries)
# ============================================================================

async def bulk_cache_set(items: Dict[str, Any], ttl: int = MATERIALIZED_VIEW_TTL):
    """
    Bulk Redis operations using pipeline - critical for millions
    """
    if redis_client is None or not items:
        return
    
    try:
        item_list = list(items.items())
        
        for i in range(0, len(item_list), REDIS_PIPELINE_SIZE):
            chunk = item_list[i:i + REDIS_PIPELINE_SIZE]
            
            pipe = redis_client.pipeline()
            for key, value in chunk:
                pipe.setex(key, ttl, json.dumps(value))
            
            await pipe.execute()
        
        logger.info(f"Bulk cached {len(items)} items")
        
    except Exception as e:
        logger.error(f"Bulk cache error: {e}")

async def bulk_cache_get(keys: List[str]) -> Dict[str, Any]:
    """
    Bulk Redis retrieval using pipeline
    """
    if redis_client is None or not keys:
        return {}
    
    try:
        results = {}
        
        for i in range(0, len(keys), REDIS_PIPELINE_SIZE):
            chunk = keys[i:i + REDIS_PIPELINE_SIZE]
            
            pipe = redis_client.pipeline()
            for key in chunk:
                pipe.get(key)
            
            values = await pipe.execute()
            
            for key, value in zip(chunk, values):
                if value:
                    results[key] = json.loads(value)
        
        return results
        
    except Exception as e:
        logger.error(f"Bulk cache get error: {e}")
        return {}

# ============================================================================
# REDIS-BACKED MATERIALIZED VIEW (for millions)
# ============================================================================

async def build_materialized_view_for_date(date_str: str) -> Dict:
    """
    Build aggregated view using STREAMING for millions of records
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = target_date.replace(hour=23, minute=59, second=59, microsecond=999)
        
        match_conditions = {"created_at": {"$gte": start, "$lte": end}}
        
        client_stats = await streaming_aggregate_by_client_status(match_conditions)
        
        if client_stats and redis_client is not None:
            key = f"{MATERIALIZED_VIEW_PREFIX}{date_str}"
            await redis_client.setex(key, MATERIALIZED_VIEW_TTL, json.dumps(client_stats))
        
        return client_stats
        
    except Exception as e:
        logger.error(f"Build view error for {date_str}: {e}")
        return {}

async def get_materialized_view_from_redis(date: str) -> Optional[Dict]:
    """Get pre-aggregated data from Redis"""
    if redis_client is None:
        return None
    
    try:
        key = f"{MATERIALIZED_VIEW_PREFIX}{date}"
        data = await redis_client.get(key)
        
        if data:
            logger.info(f"Materialized view HIT: {date}")
            return json.loads(data)
        
        return None
        
    except Exception as e:
        logger.error(f"Redis get error: {e}")
        return None

async def refresh_materialized_views_incremental():
    """
    Refresh views for recent dates using parallel processing
    """
    try:
        start_time = time.time()
        logger.info(f"Starting incremental view refresh (last {INCREMENTAL_REFRESH_DAYS} days)...")
        
        today = datetime.now()
        dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(INCREMENTAL_REFRESH_DAYS)]
        
        results = await parallel_aggregate_dates(dates, semaphore_limit=PARALLEL_DATE_LIMIT)
        
        cache_items = {f"{MATERIALIZED_VIEW_PREFIX}{date}": stats for date, stats in results.items() if stats}
        await bulk_cache_set(cache_items, ttl=MATERIALIZED_VIEW_TTL)
        
        duration = time.time() - start_time
        logger.info(f"Incremental refresh completed in {duration:.1f}s")
        
        return duration
        
    except Exception as e:
        logger.error(f"Incremental refresh error: {e}")
        return None

async def refresh_materialized_views_full():
    """
    Full refresh for last 30 days using parallel processing
    """
    try:
        start_time = time.time()
        logger.info("Starting FULL view refresh (last 30 days)...")
        
        today = datetime.now()
        dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
        
        results = await parallel_aggregate_dates(dates, semaphore_limit=PARALLEL_DATE_LIMIT)
        
        cache_items = {f"{MATERIALIZED_VIEW_PREFIX}{date}": stats for date, stats in results.items() if stats}
        await bulk_cache_set(cache_items, ttl=MATERIALIZED_VIEW_TTL)
        
        duration = time.time() - start_time
        logger.info(f"Full refresh completed in {duration:.1f}s")
        
        return duration
        
    except Exception as e:
        logger.error(f"Full refresh error: {e}")
        return None

refresh_materialized_view = refresh_materialized_views_incremental

# ============================================================================
# CACHING LAYER
# ============================================================================

async def get_cache(key: str, endpoint: str = "unknown") -> Optional[dict]:
    """Retrieve data from Redis cache with metrics"""
    if redis_client is None:
        cache_metrics["misses"] += 1
        cache_metrics["total_requests"] += 1
        return None
    
    try:
        data = await redis_client.get(key)
        cache_metrics["total_requests"] += 1
        
        if data:
            cache_metrics["hits"] += 1
            cache_metrics["by_endpoint"][endpoint]["hits"] += 1
            cache_metrics["hit_rate"] = (cache_metrics["hits"] / cache_metrics["total_requests"]) * 100
            return json.loads(data)
        else:
            cache_metrics["misses"] += 1
            cache_metrics["by_endpoint"][endpoint]["misses"] += 1
            return None
    except Exception as e:
        logger.error(f"Cache error: {e}")
        return None

async def set_cache(key: str, data: Any, expire: int = DEFAULT_CACHE_TTL):
    """Store data in Redis cache"""
    if redis_client is None:
        return
    try:
        if isinstance(data, (dict, list)):
            await redis_client.setex(key, expire, json.dumps(data))
        else:
            await redis_client.setex(key, expire, str(data))
    except Exception as e:
        logger.error(f"Cache error: {e}")

async def clear_cache_pattern(pattern: str):
    """Clear cache by pattern with batch operations"""
    if redis_client is None:
        return
    try:
        keys = await redis_client.keys(pattern)
        if keys:
            pipe = redis_client.pipeline()
            for key in keys:
                pipe.delete(key)
            await pipe.execute()
            logger.info(f"Cleared {len(keys)} cache keys")
    except Exception as e:
        logger.error(f"Cache error: {e}")

# ============================================================================
# CLIENT LIST (Optimized for millions) - WITH FALLBACK
# ============================================================================

async def get_distinct_clients_from_db(limit: int = 10000) -> List[str]:
    """
    Fallback: Get distinct clients directly from MongoDB using aggregation
    Optimized for millions of records
    """
    try:
        if collection is None:
            return []
            
        logger.info("Fetching distinct clients from database (fallback)...")
        
        # Use MongoDB's distinct operation - fast even on millions
        distinct_clients = await collection.distinct("client_id", {"client_id": {"$ne": None, "$ne": "unknown"}})
        
        # Filter and sort
        valid_clients = [str(c) for c in distinct_clients if c and str(c) != 'unknown']
        valid_clients.sort()
        
        # Limit for safety
        if len(valid_clients) > limit:
            logger.warning(f"Limiting clients to {limit} from {len(valid_clients)}")
            valid_clients = valid_clients[:limit]
        
        logger.info(f"Retrieved {len(valid_clients)} distinct clients from database")
        return valid_clients
        
    except Exception as e:
        logger.error(f"Error fetching distinct clients: {e}")
        traceback.print_exc()
        return []

async def refresh_client_list_background():
    """
    Fetch unique clients using streaming for millions of records
    """
    if collection is None:
        return
    
    try:
        logger.info("Background: Refreshing client list (streaming mode)...")
        
        unique_clients = set()
        projection = {"client_id": 1, "_id": 0}
        
        async for batch in stream_tickets({}, projection, batch_size=STREAM_BATCH_SIZE):
            for ticket in batch:
                client_id = str(ticket.get('client_id', 'unknown'))
                if client_id != 'unknown' and client_id:
                    unique_clients.add(client_id)
            
            if len(unique_clients) % 10000 == 0:
                logger.info(f"Extracted {len(unique_clients):,} unique clients...")
        
        all_clients = sorted(list(unique_clients))
        total_count = len(all_clients)
        
        await set_cache("all_clients_raw_list", all_clients, expire=CLIENT_LIST_CACHE_TTL)
        await set_cache("client_count:all", total_count, expire=CLIENT_LIST_CACHE_TTL)
        
        logger.info(f"Background: Cached {total_count:,} unique clients")
        
    except Exception as e:
        logger.error(f"Client list refresh error: {e}")
        traceback.print_exc()

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def require_db(func: Callable):
    """Decorator to check database availability and handle errors"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if mongo_client is None:
            return JSONResponse(status_code=503, content={"error": "Database unavailable"})
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            traceback.print_exc()
            return JSONResponse(status_code=500, content={"error": str(e)})
    return wrapper

async def scheduled_refresh_with_adaptive_interval():
    """Smart scheduler that adapts based on refresh duration"""
    duration = await refresh_materialized_view()
    if duration:
        next_interval_minutes = max(10, int(duration / 60 * 1.5))
        logger.info(f"Next refresh in {next_interval_minutes} minutes")
        scheduler.reschedule_job('refresh_view', trigger='interval', minutes=next_interval_minutes)

# ============================================================================
# LIFESPAN CONTEXT MANAGER (Simplified - No Index Creation)
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    Optimized for read-only analytics dashboard with millions of records
    """
    # STARTUP
    global mongo_client, db, collection, redis_client
    
    logger.info("=" * 80)
    logger.info("APPLICATION STARTUP - INITIALIZING CONNECTIONS")
    logger.info("=" * 80)
    
    # MongoDB Connection
    try:
        MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        
        mongo_client = AsyncIOMotorClient(
            MONGO_URI,
            maxPoolSize=200,
            minPoolSize=50,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=5000,
            retryWrites=False,
            retryReads=True,
            read_preference=ReadPreference.SECONDARY_PREFERRED,
            readConcernLevel="local"
        )
        
        await mongo_client.admin.command('ping')
        db = mongo_client["AutoQA"]
        collection = db[MAIN_COLLECTION_NAME]
        
        estimated_count = await collection.estimated_document_count()
        
        logger.info("CONNECTED TO MONGODB - OPTIMIZED FOR MILLIONS OF RECORDS")
        logger.info(f"Collection: {MAIN_COLLECTION_NAME}")
        logger.info(f"Estimated documents: {estimated_count:,}")
        logger.info(f"Streaming enabled: YES (batch_size={STREAM_BATCH_SIZE})")
        logger.info(f"Read preference: SECONDARY_PREFERRED")
        logger.info(f"Database mode: 100% READ ONLY")
        
        # Check existing indexes (informational only - no creation)
        try:
            indexes = await collection.index_information()
            logger.info(f"Available indexes: {list(indexes.keys())}")
            logger.info("Using existing indexes - optimized for caching strategy")
        except Exception as e:
            logger.warning(f"Could not retrieve index info: {e}")
        
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        traceback.print_exc()
        mongo_client = None
    
    # Redis Connection (Primary performance optimization)
    try:
        redis_client = await aioredis.from_url(
            "redis://localhost:6379",
            encoding="utf-8",
            decode_responses=True,
            max_connections=100
        )
        await redis_client.ping()
        
        try:
            await redis_client.config_set('maxmemory-policy', 'allkeys-lru')
        except:
            pass
        
        logger.info("Redis connected - materialized views enabled")
        logger.info("Cache strategy: Redis materialized views (faster than any index)")
        
    except Exception as e:
        logger.warning(f"⚠️  Redis connection failed: {e}")
        logger.warning("⚠️  Application will work but without caching (slower performance)")
        redis_client = None
    
    # Build initial data caches (the real performance optimization!)
    logger.info("Building initial client list...")
    await refresh_client_list_background()
    
    logger.info("Building initial materialized views (30 days)...")
    await refresh_materialized_views_full()
    
    # Start scheduler for automatic cache refresh
    if redis_client is not None:
        scheduler.add_job(
            scheduled_refresh_with_adaptive_interval,
            'interval',
            minutes=15,
            id='refresh_view'
        )
        
        scheduler.add_job(
            refresh_client_list_background,
            'interval',
            minutes=60,
            id='refresh_clients'
        )
        
        scheduler.start()
        logger.info("Scheduler started: View refresh (15min), Clients (60min)")
    else:
        logger.warning("⚠️  Scheduler disabled - Redis unavailable")
    
    logger.info("=" * 80)
    logger.info("✅ STARTUP COMPLETE - APPLICATION READY")
    logger.info("🚀 Performance: Redis caching + MongoDB streaming + No index overhead")
    logger.info("=" * 80)
    
    # Application runs here
    yield
    
    # SHUTDOWN
    logger.info("=" * 80)
    logger.info("APPLICATION SHUTDOWN - CLEANING UP")
    logger.info("=" * 80)
    
    if scheduler.running:
        scheduler.shutdown()
        logger.info("⏰ Scheduler stopped")
    
    if redis_client is not None:
        await redis_client.close()
        logger.info("💾 Redis connection closed")
    
    if mongo_client is not None:
        mongo_client.close()
        logger.info("🗄️  MongoDB connection closed")
    
    logger.info("✅ Application shutdown complete")

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ============================================================================
# API ENDPOINTS (Optimized for millions - NO MEMORY LISTS)
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Render dashboard"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/stats")
async def get_stats():
    """System statistics and health"""
    return JSONResponse(content={
        "cache": {
            "hits": cache_metrics["hits"],
            "misses": cache_metrics["misses"],
            "hit_rate": f"{cache_metrics.get('hit_rate', 0):.2f}%"
        },
        "memory": {
            "peak_batch_size": memory_stats["peak_batch_size"],
            "total_processed": memory_stats["total_processed"],
            "streaming_queries": memory_stats["streaming_queries"]
        },
        "config": {
            "stream_batch_size": STREAM_BATCH_SIZE,
            "max_memory_batch": MAX_MEMORY_BATCH,
            "parallel_date_limit": PARALLEL_DATE_LIMIT,
            "database_writes": "DISABLED - READ ONLY MODE"
        }
    })

@app.get("/api/cache-metrics")
async def get_cache_metrics():
    """Get cache hit rate metrics"""
    endpoint_stats = {}
    for endpoint, stats in cache_metrics["by_endpoint"].items():
        total = stats["hits"] + stats["misses"]
        if total > 0:
            endpoint_stats[endpoint] = {
                "hits": stats["hits"],
                "misses": stats["misses"],
                "total": total,
                "hit_rate": (stats["hits"] / total) * 100
            }
    
    return JSONResponse(content={
        "overall": {
            "hits": cache_metrics["hits"],
            "misses": cache_metrics["misses"],
            "total": cache_metrics["total_requests"],
            "hit_rate": cache_metrics["hit_rate"]
        },
        "by_endpoint": endpoint_stats
    })

@app.get("/api/ticket-data")
@require_db
@rate_limit
async def get_ticket_data(request: Request, date: str = None):
    """Get tickets by date - optimized for millions"""
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')
    
    cache_key = f"ticket_data:{date}"
    cached = await get_cache(cache_key, "ticket_data")
    if cached:
        return JSONResponse(content=cached)
    
    client_stats = await get_materialized_view_from_redis(date)
    
    if not client_stats:
        client_stats = await build_materialized_view_for_date(date)
    
    if not client_stats:
        return JSONResponse(content={
            'client_ids': [], 'completed': [], 'processing': [],
            'failed': [], 'callback': []
        })
    
    sorted_clients = sorted(client_stats.keys())[:100]
    
    response = {
        'client_ids': sorted_clients,
        'completed': [client_stats[c]['completed'] for c in sorted_clients],
        'processing': [client_stats[c]['processing'] for c in sorted_clients],
        'failed': [client_stats[c]['failed'] for c in sorted_clients],
        'callback': [client_stats[c]['callback'] for c in sorted_clients]
    }
    
    await set_cache(cache_key, response, expire=VIEW_CACHE_TTL)
    return JSONResponse(content=response)

@app.get("/api/detailed-tickets")
@require_db
@rate_limit
async def get_detailed_tickets(request: Request, date: str, status: str):
    """
    Get detailed tickets - OPTIMIZED: No list building, uses MongoDB filtering
    Stream results directly, stop at limit
    """
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
        start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = target_date.replace(hour=23, minute=59, second=59, microsecond=999)
        
        match_conditions = {
            "created_at": {"$gte": start, "$lte": end}
        }
        
        status_filter = build_status_match_conditions(status)
        if status_filter:
            match_conditions.update(status_filter)
        
        projection = {
            "ticket_id": 1,
            "client_id": 1,
            "created_at": 1,
            "auto_qa_status": 1,
            "request_status": 1,
            "_id": 1
        }
        
        tickets = []
        max_tickets = 1000
        
        async for ticket in stream_tickets_with_limit(
            match_conditions, 
            projection, 
            max_results=max_tickets,
            sort=[("created_at", -1)]
        ):
            normalized = normalize_ticket_document(ticket)
            
            ticket_status = parse_ticket_status(normalized)
            if ticket_status == status:
                tickets.append({
                    'ticket_id': normalized.get('ticket_id', 'N/A'),
                    'client_id': str(normalized.get('client_id', 'N/A')),
                    'status': status,
                    'created_at': normalized['created_at'].strftime('%Y-%m-%d %H:%M') if isinstance(normalized.get('created_at'), datetime) else 'N/A',
                    'auto_qa_status': normalized.get('auto_qa_status', 'N/A'),
                    'request_status': normalized.get('request_status', 'N/A')
                })
            
            if len(tickets) >= max_tickets:
                break
        
        logger.info(f"Returned {len(tickets)} detailed tickets for {date}/{status}")
        
        return JSONResponse(content={
            'date': date,
            'status': status,
            'total_tickets': len(tickets),
            'tickets': tickets
        })
        
    except Exception as e:
        logger.error(f"Detailed tickets error: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/client-tickets-7days")
@require_db
@rate_limit
async def get_client_tickets_7days(request: Request, client_id: str):
    """Get client 7-day data using streaming"""
    if not client_id:
        return JSONResponse(status_code=400, content={"error": "Client ID required"})
    
    cache_key = f"client_7days:{client_id}"
    cached = await get_cache(cache_key, "client_7days")
    if cached:
        return JSONResponse(content=cached)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=6)
    start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end_date.replace(hour=23, minute=59, second=59, microsecond=999)
    
    match_conditions = {
        'client_id': str(client_id),
        'created_at': {'$gte': start, '$lte': end}
    }
    
    daily_counts = await streaming_aggregate_by_date_status(match_conditions, start_date, 7)
    
    sorted_dates = sorted(daily_counts.keys())
    response = {
        'success': True,
        'seven_day_data': {
            'dates': [datetime.strptime(d, '%Y-%m-%d').strftime('%b %d, %Y') for d in sorted_dates],
            'completed': [daily_counts[d]['completed'] for d in sorted_dates],
            'processing': [daily_counts[d]['processing'] for d in sorted_dates],
            'failed': [daily_counts[d]['failed'] for d in sorted_dates],
            'callback': [daily_counts[d]['callback'] for d in sorted_dates]
        }
    }
    
    await set_cache(cache_key, response, expire=DEFAULT_CACHE_TTL)
    return JSONResponse(content=response)

@app.get("/api/all-clients")
@require_db
@rate_limit
async def get_all_clients(request: Request, limit: int = 1000, offset: int = 0, search: str = None):
    """
    Get client list with pagination - millions safe
    WITH FALLBACK to database if cache is empty
    """
    cache_key = f"all_clients:{limit}:{offset}:{search or 'all'}"
    cached = await get_cache(cache_key, "all_clients")
    if cached:
        logger.info(f"✅ Returning cached client list: {cached.get('total', 0)} total")
        return JSONResponse(content=cached)
    
    # Get from cache
    all_clients = await get_cache("all_clients_raw_list", "all_clients_raw")
    
    # ✅ FALLBACK: If cache is empty, get from database directly
    if not all_clients:
        logger.warning("❌ Client cache empty - using database fallback")
        all_clients = await get_distinct_clients_from_db(limit=10000)
        
        if not all_clients:
            # Still empty - trigger background refresh and return empty
            logger.error("❌ No clients found in database - triggering refresh")
            asyncio.create_task(refresh_client_list_background())
            return JSONResponse(content={
                "client_ids": [], 
                "total": 0, 
                "message": "Building client list... Please refresh in a moment."
            })
        
        # Cache the fallback result
        await set_cache("all_clients_raw_list", all_clients, expire=CLIENT_LIST_CACHE_TTL)
        logger.info(f"✅ Cached fallback client list: {len(all_clients)} clients")
    
    # Search filter
    if search:
        query_lower = search.lower()
        all_clients = [c for c in all_clients if query_lower in c.lower()]
    
    total = len(all_clients)
    paginated = all_clients[offset:offset + limit]
    
    response = {
        "client_ids": paginated,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + len(paginated)) < total
    }
    
    await set_cache(cache_key, response, expire=CLIENT_LIST_CACHE_TTL)
    logger.info(f"✅ Returning {len(paginated)} clients (total: {total})")
    return JSONResponse(content=response)

@app.get("/api/clients/search")
@require_db
@rate_limit
async def search_clients(request: Request, q: str, limit: int = 50):
    """
    Fast client search using cached data
    WITH FALLBACK to database if cache is empty
    """
    if not q or len(q) < 1:
        return JSONResponse(content={"client_ids": []})
    
    cache_key = f"client_search:{q}:{limit}"
    cached = await get_cache(cache_key, "client_search")
    if cached:
        return JSONResponse(content=cached)
    
    try:
        # Get from cache
        all_clients = await get_cache("all_clients_raw_list", "all_clients_raw")
        
        # ✅ FALLBACK: If cache is empty, get from database
        if not all_clients:
            logger.warning("Client cache empty for search - using database fallback")
            all_clients = await get_distinct_clients_from_db(limit=10000)
            
            if not all_clients:
                asyncio.create_task(refresh_client_list_background())
                return JSONResponse(content={"client_ids": []})
            
            # Cache for next time
            await set_cache("all_clients_raw_list", all_clients, expire=CLIENT_LIST_CACHE_TTL)
        
        # Search in memory
        query_lower = q.lower()
        matching = [c for c in all_clients if query_lower in c.lower()][:limit]
        
        response = {"client_ids": matching}
        await set_cache(cache_key, response, expire=600)
        
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Client search error: {e}")
        traceback.print_exc()
        return JSONResponse(content={"client_ids": []})

@app.get("/api/ticket-data-range")
@require_db
@rate_limit
async def get_ticket_data_range(request: Request, start_date: str, end_date: str, client_ids: str = None):
    """
    Get ticket data for date range using streaming for millions
    Supports optional client filtering - NO GIANT LISTS
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999)
        
        cache_key = f"range:{start_date}:{end_date}:{client_ids or 'all'}"
        cached = await get_cache(cache_key, "ticket_data_range")
        if cached:
            return JSONResponse(content=cached)
        
        match_conditions = {"created_at": {"$gte": start, "$lte": end}}
        
        if client_ids:
            client_list = [c.strip() for c in client_ids.split(",")]
            match_conditions["client_id"] = {"$in": client_list}
        
        projection = {
            "created_at": 1,
            "client_id": 1,
            "auto_qa_status": 1,
            "request_status": 1,
            "_id": 0
        }
        
        total_stats = init_stats_dict()
        daily_data = {}
        client_breakdown = {}
        total_processed = 0
        
        async for batch in stream_tickets(match_conditions, projection, batch_size=2000):
            for ticket in batch:
                normalized = normalize_ticket_document(ticket)
                date_str = extract_date(normalized).strftime('%Y-%m-%d')
                client_id = str(normalized.get('client_id', 'unknown'))
                status = parse_ticket_status(normalized)
                
                if status in total_stats:
                    total_stats[status] += 1
                
                if date_str not in daily_data:
                    daily_data[date_str] = init_stats_dict()
                if status in daily_data[date_str]:
                    daily_data[date_str][status] += 1
                
                if client_id not in client_breakdown:
                    client_breakdown[client_id] = {**init_stats_dict(), "total": 0}
                if status in client_breakdown[client_id]:
                    client_breakdown[client_id][status] += 1
                    client_breakdown[client_id]["total"] += 1
            
            total_processed += len(batch)
            
            if total_processed % 50000 == 0:
                logger.info(f"Range query: processed {total_processed:,} tickets")
        
        dates = sorted(daily_data.keys())
        daily_breakdown = {
            "dates": [datetime.strptime(d, "%Y-%m-%d").strftime("%b %d") for d in dates] if dates else [],
            "completed": [daily_data[d]["completed"] for d in dates] if dates else [],
            "processing": [daily_data[d]["processing"] for d in dates] if dates else [],
            "failed": [daily_data[d]["failed"] for d in dates] if dates else [],
            "callback": [daily_data[d]["callback"] for d in dates] if dates else []
        }
        
        response = {
            "success": True,
            "total_days": (end - start).days + 1,
            "total_tickets": total_processed,
            "total_stats": total_stats,
            "daily_breakdown": daily_breakdown,
            "client_breakdown": client_breakdown
        }
        
        await set_cache(cache_key, response, expire=VIEW_CACHE_TTL)
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Range query error: {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/refresh-view")
async def manual_refresh():
    """Manually trigger incremental view refresh"""
    try:
        asyncio.create_task(refresh_materialized_view())
        return JSONResponse(content={"status": "started", "message": "Incremental refresh started in background (7 days)"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/refresh-view-full")
async def manual_refresh_view_full():
    """Manually trigger FULL materialized view refresh"""
    try:
        asyncio.create_task(refresh_materialized_views_full())
        return JSONResponse(content={
            "status": "started",
            "message": "Full refresh started in background (30 days)"
        })
    except Exception as e:
        logger.error(f"Full refresh error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/reload-data")
@require_db
async def reload_data():
    """Reload - clear cache and refresh"""
    await clear_cache_pattern("*")
    asyncio.create_task(refresh_materialized_view())
    asyncio.create_task(refresh_client_list_background())
    return JSONResponse(content={"status": "success", "message": "Cache cleared and refresh initiated"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, log_level="info", workers=4, reload=False)
