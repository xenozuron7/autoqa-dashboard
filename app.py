from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import redis.asyncio as aioredis
from datetime import datetime, timedelta
from dateutil import parser
import json
import time
from typing import Dict, List, Optional, Callable, Any
import logging
import traceback
import asyncio
from functools import wraps
from collections import defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global connection pools
mongo_client = None
db = None
collection = None
materialized_view = None
redis_client = None
scheduler = AsyncIOScheduler()

# Constants
MAIN_COLLECTION_NAME = "auto_qa_results_demo"
VIEW_COLLECTION_NAME = "ticket_summary_materialized_view"
DEFAULT_CACHE_TTL = 300
VIEW_CACHE_TTL = 600
CLIENT_LIST_CACHE_TTL = 3600
CLIENT_LIST_DEFAULT_LIMIT = 1000
INCREMENTAL_REFRESH_DAYS = 7

# Rate limiting
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 60
rate_limit_storage = defaultdict(list)

# Cache metrics
cache_metrics = {
    "hits": 0,
    "misses": 0,
    "total_requests": 0,
    "hit_rate": 0.0,
    "by_endpoint": defaultdict(lambda: {"hits": 0, "misses": 0})
}

# ============================================================================
# RATE LIMITING MIDDLEWARE
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
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "message": f"Maximum {RATE_LIMIT_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds"
                }
            )
        
        return await func(*args, **kwargs)
    return wrapper

# ============================================================================
# DATA VALIDATION & NORMALIZATION (Handle structured + unstructured data)
# ============================================================================

def safe_get_field(document: dict, field: str, default=None, expected_type=None):
    """
    Safely extract field from document with type checking
    Handles missing fields, wrong types, nested data
    """
    try:
        value = document.get(field, default)
        
        if value is None:
            return default
        
        if expected_type == str:
            return str(value)
        elif expected_type == int:
            return int(value) if isinstance(value, (int, float)) else default
        elif expected_type == datetime:
            if isinstance(value, datetime):
                return value
            elif isinstance(value, str):
                return parser.parse(value)
            else:
                return default
        
        return value
        
    except Exception as e:
        logger.warning(f"Failed to extract {field}: {e}")
        return default

def normalize_ticket_document(raw_doc: dict) -> dict:
    """
    Normalize unstructured/semi-structured ticket document
    Handles missing fields, wrong types, nested structures
    """
    try:
        # Extract created_at
        created_at = None
        
        if 'created_at' in raw_doc:
            created_at_raw = raw_doc['created_at']
            
            if isinstance(created_at_raw, datetime):
                created_at = created_at_raw
            elif isinstance(created_at_raw, str):
                try:
                    created_at = parser.parse(created_at_raw)
                except:
                    pass
            elif isinstance(created_at_raw, dict) and '$date' in created_at_raw:
                try:
                    created_at = parser.parse(created_at_raw['$date'])
                except:
                    pass
        
        if created_at is None:
            if 'updated_at' in raw_doc:
                created_at = safe_get_field(raw_doc, 'updated_at', datetime.now(), datetime)
            else:
                created_at = datetime.now()
        
        # Extract client_id
        client_id = safe_get_field(raw_doc, 'client_id', 'unknown', str)
        
        if client_id == 'unknown' and 'client' in raw_doc:
            if isinstance(raw_doc['client'], dict):
                client_id = safe_get_field(raw_doc['client'], 'id', 'unknown', str)
            elif isinstance(raw_doc['client'], str):
                client_id = raw_doc['client']
        
        # Extract status fields
        auto_qa_status = safe_get_field(raw_doc, 'auto_qa_status', '', str)
        request_status = safe_get_field(raw_doc, 'request_status', '', str)
        
        # Check nested status
        if not auto_qa_status and 'qa' in raw_doc:
            if isinstance(raw_doc['qa'], dict):
                auto_qa_status = safe_get_field(raw_doc['qa'], 'status', '', str)
        
        if not request_status and 'request' in raw_doc:
            if isinstance(raw_doc['request'], dict):
                request_status = safe_get_field(raw_doc['request'], 'status', '', str)
        
        # Extract ticket_id
        ticket_id = safe_get_field(raw_doc, 'ticket_id', None, str)
        if not ticket_id:
            ticket_id = safe_get_field(raw_doc, '_id', 'unknown', str)
        
        return {
            'ticket_id': ticket_id,
            'client_id': client_id,
            'created_at': created_at,
            'auto_qa_status': auto_qa_status,
            'request_status': request_status,
            'normalized': True
        }
        
    except Exception as e:
        logger.error(f"Failed to normalize document: {e}")
        return {
            'ticket_id': 'error',
            'client_id': 'unknown',
            'created_at': datetime.now(),
            'auto_qa_status': '',
            'request_status': '',
            'normalized': False
        }

def parse_ticket_status(ticket: dict) -> str:
    """Determine ticket status from auto_qa_status"""
    auto_qa_status = str(ticket.get('auto_qa_status', '')).lower()
    request_status = str(ticket.get('request_status', '')).lower()
    
    if auto_qa_status == 'complete':
        return 'completed'
    elif auto_qa_status == 'callback':
        return 'callback'
    elif auto_qa_status == 'failed' or request_status == 'failed':
        return 'failed'
    return 'processing'

def extract_date(ticket: dict) -> datetime:
    """Extract and normalize date from ticket"""
    date_obj = ticket.get('created_at') or ticket.get('updated_at')
    
    if not date_obj:
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if isinstance(date_obj, datetime):
        return date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if isinstance(date_obj, str):
        try:
            return parser.parse(date_obj).replace(hour=0, minute=0, second=0, microsecond=0)
        except:
            pass
    
    if isinstance(date_obj, dict) and '$date' in date_obj:
        try:
            return parser.parse(date_obj['$date']).replace(hour=0, minute=0, second=0, microsecond=0)
        except:
            pass
    
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_date_range(end_date: datetime, days: int = 7) -> tuple:
    """Get start and end datetime for date range"""
    start = (end_date - timedelta(days=days-1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = end_date.replace(hour=23, minute=59, second=59, microsecond=999)
    return start, end

def format_date_labels(end_date: datetime, days: int = 7) -> List[str]:
    """Generate date labels for charts"""
    return [(end_date - timedelta(days=i)).strftime('%b %d, %Y') for i in range(days-1, -1, -1)]

def init_stats_dict() -> dict:
    """Initialize empty stats dictionary"""
    return {'completed': 0, 'processing': 0, 'failed': 0, 'callback': 0}

# ============================================================================
# DATABASE READ OPERATIONS (ONLY collection.find() - NO AGGREGATION)
# ============================================================================

async def fetch_tickets_raw(
    match_conditions: dict,
    projection: dict = None,
    limit: int = 10000,
    sort: List[tuple] = None
) -> List[dict]:
    """
    Fetch tickets using ONLY collection.find()
    NO aggregation, NO distinct - pure find query
    """
    try:
        query = collection.find(match_conditions, projection)
        
        if sort:
            query = query.sort(sort)
        
        if limit:
            query = query.limit(limit)
        
        # Fetch raw documents from database
        tickets_data = await query.to_list(length=limit)
        
        logger.info(f"Fetched {len(tickets_data)} raw tickets using collection.find()")
        return tickets_data
        
    except Exception as e:
        logger.error(f"Failed to fetch tickets: {e}")
        return []

async def fetch_view_raw(
    match_conditions: dict,
    projection: dict = None,
    limit: int = 10000,
    sort: List[tuple] = None
) -> List[dict]:
    """
    Fetch from materialized view using ONLY .find()
    NO aggregation
    """
    if materialized_view is None:
        return []
    
    try:
        query = materialized_view.find(match_conditions, projection)
        
        if sort:
            query = query.sort(sort)
        
        if limit:
            query = query.limit(limit)
        
        view_data = await query.to_list(length=limit)
        
        logger.info(f"Fetched {len(view_data)} raw entries using view.find()")
        return view_data
        
    except Exception as e:
        logger.error(f"Failed to fetch from view: {e}")
        return []

# ============================================================================
# IN-MEMORY TRANSFORMATION FUNCTIONS (PURE - NO DATABASE ACCESS)
# ============================================================================

def normalize_ticket_list(raw_tickets: List[dict]) -> List[dict]:
    """
    Normalize list of tickets handling structured/unstructured data
    Returns new list of normalized tickets
    """
    normalized = []
    for ticket in raw_tickets:
        normalized.append(normalize_ticket_document(ticket))
    return normalized

def count_by_status(data: List[dict]) -> Dict[str, int]:
    """Count items by status in memory"""
    counts = init_stats_dict()
    
    for item in data:
        status = parse_ticket_status(item)
        if status in counts:
            counts[status] += 1
    
    return counts

def count_by_client_and_status(data: List[dict]) -> Dict[str, Dict[str, int]]:
    """
    Count items by client and status in memory
    Returns: {client_id: {status: count}}
    """
    client_counts = {}
    
    for item in data:
        client_id = str(item.get('client_id', 'unknown'))
        status = parse_ticket_status(item)
        
        if client_id not in client_counts:
            client_counts[client_id] = init_stats_dict()
        
        client_counts[client_id][status] += 1
    
    return client_counts

def count_by_date_and_status(data: List[dict], start_date: datetime, days: int) -> Dict[str, Dict[str, int]]:
    """
    Count items by date and status in memory
    Returns: {date_str: {status: count}}
    """
    daily_counts = {}
    
    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        daily_counts[date_str] = init_stats_dict()
    
    for item in data:
        item_date = extract_date(item)
        date_str = item_date.strftime('%Y-%m-%d')
        
        if date_str in daily_counts:
            status = parse_ticket_status(item)
            if status in daily_counts[date_str]:
                daily_counts[date_str][status] += 1
    
    return daily_counts

def filter_by_status(data: List[dict], target_status: str) -> List[dict]:
    """Filter in-memory data by status"""
    return [item for item in data if parse_ticket_status(item) == target_status]

def filter_by_client(data: List[dict], client_id: str) -> List[dict]:
    """Filter in-memory data by client_id"""
    return [item for item in data if str(item.get('client_id', '')) == str(client_id)]

def filter_by_date_range(data: List[dict], start: datetime, end: datetime) -> List[dict]:
    """Filter in-memory data by date range"""
    filtered = []
    for item in data:
        item_date = extract_date(item)
        if start <= item_date <= end:
            filtered.append(item)
    return filtered

def sort_by_date(data: List[dict], descending: bool = True) -> List[dict]:
    """Sort in-memory data by date"""
    sorted_data = data.copy()
    sorted_data.sort(key=lambda x: extract_date(x), reverse=descending)
    return sorted_data

def extract_unique_values(data: List[dict], field: str) -> List[str]:
    """Extract unique values for a field from in-memory data"""
    unique_values = set()
    for item in data:
        value = item.get(field)
        if value is not None:
            unique_values.add(str(value))
    return sorted(list(unique_values))

def paginate_data(data: List[Any], offset: int, limit: int) -> List[Any]:
    """Paginate in-memory data"""
    return data[offset:offset + limit]

def search_in_list(items: List[str], query: str) -> List[str]:
    """Search for items matching query"""
    query_lower = query.lower()
    return [item for item in items if query_lower in item.lower()]

# ============================================================================
# CACHING LAYER WITH METRICS
# ============================================================================

async def get_cache(key: str, endpoint: str = "unknown") -> Optional[dict]:
    """Retrieve data from Redis cache with metrics"""
    if redis_client is None:
        cache_metrics["misses"] += 1
        cache_metrics["total_requests"] += 1
        cache_metrics["by_endpoint"][endpoint]["misses"] += 1
        return None
    
    try:
        data = await redis_client.get(key)
        cache_metrics["total_requests"] += 1
        
        if data:
            cache_metrics["hits"] += 1
            cache_metrics["by_endpoint"][endpoint]["hits"] += 1
            cache_metrics["hit_rate"] = (cache_metrics["hits"] / cache_metrics["total_requests"]) * 100
            logger.info(f"CACHE HIT: {key} (endpoint: {endpoint})")
            return json.loads(data)
        else:
            cache_metrics["misses"] += 1
            cache_metrics["by_endpoint"][endpoint]["misses"] += 1
            cache_metrics["hit_rate"] = (cache_metrics["hits"] / cache_metrics["total_requests"]) * 100
            return None
    except Exception as e:
        logger.error(f"Cache error: {e}")
        cache_metrics["misses"] += 1
        cache_metrics["total_requests"] += 1
        return None

async def set_cache(key: str, data: dict, expire: int = DEFAULT_CACHE_TTL):
    """Store data in Redis cache"""
    if redis_client is None:
        return
    try:
        await redis_client.setex(key, expire, json.dumps(data))
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
# CACHE WARMING
# ============================================================================

async def warm_cache():
    """Pre-populate cache with hot data on startup"""
    if mongo_client is None or redis_client is None:
        logger.warning("Cannot warm cache - connections not ready")
        return
    
    try:
        logger.info("Starting cache warming...")
        start_time = time.time()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Warm today's data
        if materialized_view is not None:
            raw_view_data = await fetch_view_raw(
                match_conditions={"date": today},
                projection={"client_id": 1, "status": 1, "count": 1, "_id": 0},
                limit=5000,
                sort=[("client_id", 1)]
            )
            
            if raw_view_data:
                client_stats = {}
                for entry in raw_view_data:
                    client_id = entry.get("client_id")
                    status = entry.get("status")
                    count = entry.get("count", 0)
                    
                    if client_id not in client_stats:
                        client_stats[client_id] = init_stats_dict()
                    
                    if status in client_stats[client_id]:
                        client_stats[client_id][status] += count
                
                sorted_clients = sorted(client_stats.keys())[:100]
                response_data = {
                    'client_ids': sorted_clients,
                    'completed': [client_stats[c]['completed'] for c in sorted_clients],
                    'processing': [client_stats[c]['processing'] for c in sorted_clients],
                    'failed': [client_stats[c]['failed'] for c in sorted_clients],
                    'callback': [client_stats[c]['callback'] for c in sorted_clients]
                }
                await set_cache(f"ticket_data:{today}", response_data, expire=VIEW_CACHE_TTL)
                logger.info("Warmed: Today's ticket data")
        
        # Warm client list
        await refresh_client_list_background()
        logger.info("Warmed: Client list")
        
        # Warm last 3 days
        for i in range(1, 4):
            past_date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            if materialized_view is not None:
                raw_view_data = await fetch_view_raw(
                    match_conditions={"date": past_date},
                    projection={"client_id": 1, "status": 1, "count": 1, "_id": 0},
                    limit=5000,
                    sort=[("client_id", 1)]
                )
                
                if raw_view_data:
                    client_stats = {}
                    for entry in raw_view_data:
                        client_id = entry.get("client_id")
                        status = entry.get("status")
                        count = entry.get("count", 0)
                        
                        if client_id not in client_stats:
                            client_stats[client_id] = init_stats_dict()
                        
                        if status in client_stats[client_id]:
                            client_stats[client_id][status] += count
                    
                    sorted_clients = sorted(client_stats.keys())[:100]
                    response_data = {
                        'client_ids': sorted_clients,
                        'completed': [client_stats[c]['completed'] for c in sorted_clients],
                        'processing': [client_stats[c]['processing'] for c in sorted_clients],
                        'failed': [client_stats[c]['failed'] for c in sorted_clients],
                        'callback': [client_stats[c]['callback'] for c in sorted_clients]
                    }
                    await set_cache(f"ticket_data:{past_date}", response_data, expire=VIEW_CACHE_TTL)
        
        logger.info("Warmed: Last 3 days data")
        
        duration = time.time() - start_time
        logger.info(f"Cache warming completed in {duration:.2f}s")
        
    except Exception as e:
        logger.error(f"Cache warming failed: {e}")
        traceback.print_exc()

# ============================================================================
# DATABASE HELPERS
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

def build_date_match(start: datetime, end: datetime) -> dict:
    """Build date match query"""
    return {"created_at": {"$gte": start, "$lte": end}}

# ============================================================================
# CLIENT LIST WITH SEARCH
# ============================================================================

async def refresh_client_list_background():
    """Background task - fetch all clients using only find()"""
    if collection is None:
        return
    
    try:
        logger.info("Background: Refreshing client list...")
        
        if materialized_view is not None:
            # Fetch all view entries using only find()
            raw_view_data = await fetch_view_raw(
                match_conditions={},
                projection={"client_id": 1, "_id": 0},
                limit=50000,
                sort=None
            )
            
            # Extract unique clients in memory
            all_clients = extract_unique_values(raw_view_data, "client_id")
            total_count = len(all_clients)
            clients = all_clients[:CLIENT_LIST_DEFAULT_LIMIT]
        else:
            # Fetch all tickets using only find() and extract clients in memory
            raw_tickets = await fetch_tickets_raw(
                match_conditions={},
                projection={"client_id": 1, "_id": 0},
                limit=50000,
                sort=None
            )
            
            # Extract unique clients in memory
            all_clients = extract_unique_values(raw_tickets, "client_id")
            total_count = len(all_clients)
            clients = all_clients[:CLIENT_LIST_DEFAULT_LIMIT]
        
        response_data = {
            "client_ids": clients,
            "total": total_count,
            "limit": CLIENT_LIST_DEFAULT_LIMIT,
            "offset": 0,
            "has_more": len(clients) < total_count
        }
        
        if redis_client is not None:
            cache_key = f"all_clients:{CLIENT_LIST_DEFAULT_LIMIT}:0:all"
            await set_cache(cache_key, response_data, expire=CLIENT_LIST_CACHE_TTL)
            await set_cache("client_count:all", total_count, expire=CLIENT_LIST_CACHE_TTL)
            await set_cache("all_clients_raw_list", all_clients, expire=CLIENT_LIST_CACHE_TTL)
        
        logger.info(f"Background: Cached {len(clients)}/{total_count} clients")
        
    except Exception as e:
        logger.error(f"Background client refresh failed: {e}")

# ============================================================================
# MATERIALIZED VIEW (Using ONLY collection.find() + in-memory aggregation)
# ============================================================================

async def refresh_materialized_view_incremental():
    """
    Incremental refresh using ONLY collection.find()
    All aggregation done in memory
    """
    if collection is None or materialized_view is None:
        logger.warning("Collections not initialized")
        return
    
    try:
        start_time = time.time()
        logger.info(f"Starting incremental view refresh (last {INCREMENTAL_REFRESH_DAYS} days)...")
        
        if collection.name != MAIN_COLLECTION_NAME or materialized_view.name != VIEW_COLLECTION_NAME:
            logger.error("SAFETY: Wrong collection names")
            return
        
        cutoff_date = datetime.now() - timedelta(days=INCREMENTAL_REFRESH_DAYS)
        
        # STEP 1: Fetch using ONLY collection.find()
        match_conditions = {"created_at": {"$gte": cutoff_date}}
        projection = {
            "created_at": 1,
            "updated_at": 1,
            "client_id": 1,
            "client": 1,
            "auto_qa_status": 1,
            "request_status": 1,
            "qa": 1,
            "request": 1,
            "_id": 0
        }
        
        raw_tickets = await fetch_tickets_raw(
            match_conditions=match_conditions,
            projection=projection,
            limit=None
        )
        
        logger.info(f"Fetched {len(raw_tickets)} raw tickets using collection.find()")
        
        # STEP 2: Normalize in memory
        normalized_tickets = normalize_ticket_list(raw_tickets)
        
        # STEP 3: Aggregate in memory (not in database)
        aggregated_data_dict = {}
        
        for ticket in normalized_tickets:
            date_str = extract_date(ticket).strftime('%Y-%m-%d')
            client_id = str(ticket.get('client_id', 'unknown'))
            status = parse_ticket_status(ticket)
            
            key = (date_str, client_id, status)
            
            if key not in aggregated_data_dict:
                aggregated_data_dict[key] = {
                    "date": date_str,
                    "client_id": client_id,
                    "status": status,
                    "count": 0
                }
            
            aggregated_data_dict[key]["count"] += 1
        
        # STEP 4: Convert to list
        aggregated_data = list(aggregated_data_dict.values())
        
        logger.info(f"Aggregated {len(aggregated_data)} entries in memory")
        
        # STEP 5: Delete old entries
        date_strs = [(cutoff_date + timedelta(days=i)).strftime('%Y-%m-%d') 
                     for i in range(INCREMENTAL_REFRESH_DAYS + 1)]
        delete_result = await materialized_view.delete_many({"date": {"$in": date_strs}})
        logger.info(f"Deleted {delete_result.deleted_count} old entries")
        
        # STEP 6: Insert new data
        if aggregated_data:
            batch_size = 1000
            for i in range(0, len(aggregated_data), batch_size):
                batch = aggregated_data[i:i+batch_size]
                await materialized_view.insert_many(batch, ordered=False)
        
        duration = time.time() - start_time
        logger.info(f"Incremental view refresh completed in {duration:.1f}s")
        
        if redis_client is not None:
            await clear_cache_pattern("*")
        
        main_count = await collection.count_documents({})
        view_count = await materialized_view.count_documents({})
        logger.info(f"Main: {main_count:,} docs | View: {view_count:,} entries")
        
        return duration
        
    except Exception as e:
        logger.error(f"View refresh failed: {e}")
        traceback.print_exc()
        return None

async def refresh_materialized_view_full():
    """
    Full refresh using ONLY collection.find()
    All aggregation done in memory
    """
    if collection is None or materialized_view is None:
        logger.warning("Collections not initialized")
        return
    
    try:
        start_time = time.time()
        logger.info("Starting FULL materialized view refresh...")
        
        if collection.name != MAIN_COLLECTION_NAME or materialized_view.name != VIEW_COLLECTION_NAME:
            logger.error("SAFETY: Wrong collection names")
            return
        
        # STEP 1: Fetch ALL tickets using ONLY collection.find()
        projection = {
            "created_at": 1,
            "updated_at": 1,
            "client_id": 1,
            "client": 1,
            "auto_qa_status": 1,
            "request_status": 1,
            "qa": 1,
            "request": 1,
            "_id": 0
        }
        
        raw_tickets = await fetch_tickets_raw(
            match_conditions={},
            projection=projection,
            limit=None
        )
        
        logger.info(f"Fetched {len(raw_tickets)} total tickets using collection.find()")
        
        # STEP 2: Normalize in memory
        normalized_tickets = normalize_ticket_list(raw_tickets)
        
        # STEP 3: Aggregate in memory
        aggregated_data_dict = {}
        
        for ticket in normalized_tickets:
            date_str = extract_date(ticket).strftime('%Y-%m-%d')
            client_id = str(ticket.get('client_id', 'unknown'))
            status = parse_ticket_status(ticket)
            
            key = (date_str, client_id, status)
            
            if key not in aggregated_data_dict:
                aggregated_data_dict[key] = {
                    "date": date_str,
                    "client_id": client_id,
                    "status": status,
                    "count": 0
                }
            
            aggregated_data_dict[key]["count"] += 1
        
        aggregated_data = list(aggregated_data_dict.values())
        
        logger.info(f"Aggregated {len(aggregated_data)} entries in memory")
        
        # STEP 4: Clear and insert
        await materialized_view.delete_many({})
        logger.info("Cleared entire view")
        
        if aggregated_data:
            batch_size = 1000
            for i in range(0, len(aggregated_data), batch_size):
                batch = aggregated_data[i:i+batch_size]
                await materialized_view.insert_many(batch, ordered=False)
        
        duration = time.time() - start_time
        logger.info(f"Full view refresh completed in {duration:.1f}s")
        
        if redis_client is not None:
            await clear_cache_pattern("*")
        
        return duration
        
    except Exception as e:
        logger.error(f"Full view refresh failed: {e}")
        traceback.print_exc()
        return None

refresh_materialized_view = refresh_materialized_view_incremental

async def scheduled_refresh_with_adaptive_interval():
    """Smart scheduler that adapts based on refresh duration"""
    duration = await refresh_materialized_view()
    
    if duration:
        next_interval_minutes = max(10, int(duration / 60 * 1.5))
        logger.info(f"Next refresh scheduled in {next_interval_minutes} minutes")
        scheduler.reschedule_job('refresh_view', trigger='interval', minutes=next_interval_minutes)

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=500)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def startup_event():
    """Initialize connections and scheduler"""
    global mongo_client, db, collection, materialized_view, redis_client
    
    try:
        MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        mongo_client = AsyncIOMotorClient(
            MONGO_URI,
            maxPoolSize=100,
            minPoolSize=20,
            maxIdleTimeMS=45000,
            serverSelectionTimeoutMS=5000,
            retryWrites=True,
            retryReads=True
        )
        
        await mongo_client.admin.command('ping')
        db = mongo_client["AutoQA"]
        collection = db[MAIN_COLLECTION_NAME]
        materialized_view = db[VIEW_COLLECTION_NAME]
        
        main_count = await collection.count_documents({})
        logger.info("Connected to MongoDB Atlas")
        logger.info(f"Main: {MAIN_COLLECTION_NAME} ({main_count:,} docs)")
        logger.info(f"View: {VIEW_COLLECTION_NAME}")
        
        # Create indexes on materialized view
        try:
            existing_indexes = await materialized_view.index_information()
            logger.info(f"Existing view indexes: {len(existing_indexes)}")
            
            view_indexes = [
                [("date", 1), ("client_id", 1), ("status", 1)],
                [("date", 1), ("status", 1)],
                [("client_id", 1)]
            ]
            
            for index_fields in view_indexes:
                index_name = "_".join([f"{f}_{d}" for f, d in index_fields])
                if index_name not in existing_indexes:
                    await materialized_view.create_index(index_fields, background=True)
                    logger.info(f"Created view index: {index_name}")
            
            logger.info("Materialized view indexed")
        except Exception as e:
            logger.warning(f"View indexing skipped: {e}")
        
        # PHASE 1: Create compound index on main collection
        try:
            main_indexes = await collection.index_information()
            
            if "client_id_1_created_at_-1_auto_qa_status_1" not in main_indexes:
                await collection.create_index(
                    [("client_id", 1), ("created_at", -1), ("auto_qa_status", 1)],
                    background=True,
                    name="client_id_1_created_at_-1_auto_qa_status_1"
                )
                logger.info("PHASE 1: Created compound index (client_id + created_at + auto_qa_status)")
            
            cutoff_date = datetime.now() - timedelta(days=180)
            
            if "created_at_-1_client_id_1_partial" not in main_indexes:
                await collection.create_index(
                    [("created_at", -1), ("client_id", 1)],
                    partialFilterExpression={"created_at": {"$gte": cutoff_date}},
                    background=True,
                    name="created_at_-1_client_id_1_partial"
                )
                logger.info("Created partial index (created_at + client_id, last 180 days)")
            
            if "client_id_1_only" not in main_indexes:
                await collection.create_index(
                    [("client_id", 1)],
                    background=True,
                    name="client_id_1_only"
                )
                logger.info("Created single-field index (client_id)")
            
            logger.info("Main collection indexed with compound indexes")
        except Exception as e:
            logger.warning(f"Main collection indexing skipped: {e}")
        
        # Initial view refresh
        logger.info("Performing initial view refresh...")
        await refresh_materialized_view_full()
        
        # Schedule adaptive refresh
        scheduler.add_job(
            scheduled_refresh_with_adaptive_interval,
            'interval',
            minutes=10,
            id='refresh_view',
            replace_existing=True
        )
        
        # Background client list refresh
        scheduler.add_job(
            refresh_client_list_background,
            'interval',
            seconds=1800,
            id='refresh_clients',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Adaptive scheduler started (initial: 10 min)")
        logger.info("Client list background refresh scheduled (30 min)")
        
    except Exception as e:
        logger.error(f"MongoDB failed: {e}")
        traceback.print_exc()
        mongo_client = None
    
    # Redis connection with LRU eviction policy
    try:
        redis_client = await aioredis.from_url(
            "redis://localhost:6379",
            encoding="utf-8",
            decode_responses=True,
            max_connections=50
        )
        await redis_client.ping()
        
        try:
            await redis_client.config_set('maxmemory-policy', 'allkeys-lru')
            logger.info("Redis LRU eviction policy enabled")
        except:
            logger.warning("Could not set Redis eviction policy (may need admin rights)")
        
        logger.info("Redis connected")
        
        # PHASE 1: Cache warming on startup
        await warm_cache()
        
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        redis_client = None

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections"""
    if scheduler.running:
        scheduler.shutdown()
    if redis_client is not None:
        await redis_client.close()
    if mongo_client is not None:
        mongo_client.close()

# ============================================================================
# API ENDPOINTS (ALL using collection.find() only)
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Render dashboard"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

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
    """Get tickets by date using only find()"""
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')
    
    cache_key = f"ticket_data:{date}"
    cached_data = await get_cache(cache_key, endpoint="ticket_data")
    if cached_data:
        return JSONResponse(content=cached_data)
    
    # STEP 1: FETCH using only find()
    if materialized_view is not None:
        raw_view_data = await fetch_view_raw(
            match_conditions={"date": date},
            projection={"client_id": 1, "status": 1, "count": 1, "_id": 0},
            limit=10000,
            sort=[("client_id", 1)]
        )
        
        if raw_view_data:
            # STEP 2: TRANSFORM in memory
            client_stats = {}
            
            for entry in raw_view_data:
                client_id = entry.get("client_id")
                status = entry.get("status")
                count = entry.get("count", 0)
                
                if client_id not in client_stats:
                    client_stats[client_id] = init_stats_dict()
                
                if status in client_stats[client_id]:
                    client_stats[client_id][status] += count
            
            sorted_clients = sorted(client_stats.keys())[:100]
            
            response_data = {
                'client_ids': sorted_clients,
                'completed': [client_stats[c]['completed'] for c in sorted_clients],
                'processing': [client_stats[c]['processing'] for c in sorted_clients],
                'failed': [client_stats[c]['failed'] for c in sorted_clients],
                'callback': [client_stats[c]['callback'] for c in sorted_clients]
            }
            
            await set_cache(cache_key, response_data, expire=VIEW_CACHE_TTL)
            return JSONResponse(content=response_data)
    
    # Fallback: use collection.find()
    target_date = datetime.strptime(date, "%Y-%m-%d")
    start, end = get_date_range(target_date, days=1)
    
    match_conditions = {"created_at": {"$gte": start, "$lte": end}}
    projection = {
        "client_id": 1,
        "client": 1,
        "auto_qa_status": 1,
        "request_status": 1,
        "qa": 1,
        "request": 1,
        "_id": 0
    }
    
    raw_tickets = await fetch_tickets_raw(
        match_conditions=match_conditions,
        projection=projection,
        limit=10000
    )
    
    # Normalize and transform in memory
    normalized_tickets = normalize_ticket_list(raw_tickets)
    client_counts = count_by_client_and_status(normalized_tickets)
    
    sorted_clients = sorted(client_counts.keys())[:100]
    
    response_data = {
        'client_ids': sorted_clients,
        'completed': [client_counts[c]['completed'] for c in sorted_clients],
        'processing': [client_counts[c]['processing'] for c in sorted_clients],
        'failed': [client_counts[c]['failed'] for c in sorted_clients],
        'callback': [client_counts[c]['callback'] for c in sorted_clients]
    }
    
    await set_cache(cache_key, response_data, expire=VIEW_CACHE_TTL)
    return JSONResponse(content=response_data)

@app.get("/api/detailed-tickets")
@require_db
@rate_limit
async def get_detailed_tickets(request: Request, date: str, status: str):
    """Get detailed tickets using only collection.find()"""
    target_date = datetime.strptime(date, "%Y-%m-%d")
    start, end = get_date_range(target_date, days=1)
    
    # Fetch using only find()
    match_conditions = {"created_at": {"$gte": start, "$lte": end}}
    projection = {
        "ticket_id": 1,
        "client_id": 1,
        "client": 1,
        "created_at": 1,
        "auto_qa_status": 1,
        "request_status": 1,
        "qa": 1,
        "request": 1,
        "_id": 1
    }
    
    raw_tickets = await fetch_tickets_raw(
        match_conditions=match_conditions,
        projection=projection,
        limit=5000
    )
    
    # Normalize, filter, and sort in memory
    normalized_tickets = normalize_ticket_list(raw_tickets)
    filtered_tickets = filter_by_status(normalized_tickets, status)
    sorted_tickets = sort_by_date(filtered_tickets, descending=True)
    
    # Format response
    formatted_tickets = []
    for t in sorted_tickets[:1000]:
        formatted_tickets.append({
            'ticket_id': t.get('ticket_id', 'N/A'),
            'client_id': str(t.get('client_id', 'N/A')),
            'status': status,
            'created_at': t['created_at'].strftime('%Y-%m-%d %H:%M') if isinstance(t.get('created_at'), datetime) else 'N/A',
            'auto_qa_status': t.get('auto_qa_status', 'N/A'),
            'request_status': t.get('request_status', 'N/A')
        })
    
    return JSONResponse(content={
        'date': date,
        'status': status,
        'total_tickets': len(formatted_tickets),
        'tickets': formatted_tickets
    })

@app.get("/api/all-clients")
@require_db
@rate_limit
async def get_all_clients(
    request: Request,
    limit: int = CLIENT_LIST_DEFAULT_LIMIT,
    offset: int = 0,
    search: str = None
):
    """Get client IDs using only find()"""
    
    cache_key = f"all_clients:{limit}:{offset}:{search or 'all'}"
    cached_data = await get_cache(cache_key, endpoint="all_clients")
    if cached_data:
        return JSONResponse(content=cached_data)
    
    try:
        # Fetch using only find()
        if materialized_view is not None:
            raw_view_data = await fetch_view_raw(
                match_conditions={},
                projection={"client_id": 1, "_id": 0},
                limit=50000,
                sort=None
            )
            all_clients = extract_unique_values(raw_view_data, "client_id")
        else:
            raw_tickets = await fetch_tickets_raw(
                match_conditions={},
                projection={"client_id": 1, "_id": 0},
                limit=50000,
                sort=None
            )
            all_clients = extract_unique_values(raw_tickets, "client_id")
        
        # Filter and paginate in memory
        if search:
            all_clients = search_in_list(all_clients, search)
        
        total_count = len(all_clients)
        paginated_clients = paginate_data(all_clients, offset, limit)
        
        response_data = {
            "client_ids": paginated_clients,
            "total": total_count,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + len(paginated_clients)) < total_count
        }
        
        await set_cache(cache_key, response_data, expire=CLIENT_LIST_CACHE_TTL)
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        logger.error(f"Client list query failed: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/clients/search")
@require_db
@rate_limit
async def search_clients(request: Request, q: str, limit: int = 50):
    """Fast client search using cached data"""
    if not q or len(q) < 1:
        return JSONResponse(content={"client_ids": []})
    
    cache_key = f"client_search:{q}:{limit}"
    cached_data = await get_cache(cache_key, endpoint="client_search")
    if cached_data:
        return JSONResponse(content=cached_data)
    
    try:
        all_clients_cache_key = "all_clients_raw_list"
        all_clients = await get_cache(all_clients_cache_key, endpoint="all_clients_raw")
        
        if all_clients is None:
            if materialized_view is not None:
                raw_view_data = await fetch_view_raw(
                    match_conditions={},
                    projection={"client_id": 1, "_id": 0},
                    limit=50000,
                    sort=None
                )
                all_clients = extract_unique_values(raw_view_data, "client_id")
            else:
                raw_tickets = await fetch_tickets_raw(
                    match_conditions={},
                    projection={"client_id": 1, "_id": 0},
                    limit=50000,
                    sort=None
                )
                all_clients = extract_unique_values(raw_tickets, "client_id")
            
            await set_cache(all_clients_cache_key, all_clients, expire=3600)
        
        # Search in memory
        matching_clients = search_in_list(all_clients, q)[:limit]
        
        response_data = {"client_ids": matching_clients}
        await set_cache(cache_key, response_data, expire=600)
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        logger.error(f"Client search failed: {e}")
        return JSONResponse(content={"client_ids": []})

@app.get("/api/client-tickets-7days")
@require_db
@rate_limit
async def get_client_tickets_7days(request: Request, client_id: str):
    """Get client 7-day data using only collection.find()"""
    if not client_id:
        return JSONResponse(status_code=400, content={"success": False, "error": "Client ID required"})
    
    cache_key = f"client_7days:{client_id}"
    cached_data = await get_cache(cache_key, endpoint="client_7days")
    if cached_data:
        return JSONResponse(content=cached_data)
    
    end_date = datetime.now()
    start_date, end_date = get_date_range(end_date, days=7)
    
    # Fetch using only collection.find()
    match_conditions = {
        'client_id': str(client_id),
        'created_at': {'$gte': start_date, '$lte': end_date}
    }
    projection = {
        'created_at': 1,
        'auto_qa_status': 1,
        'request_status': 1,
        'qa': 1,
        'request': 1,
        '_id': 0
    }
    
    raw_tickets = await fetch_tickets_raw(
        match_conditions=match_conditions,
        projection=projection,
        limit=10000
    )
    
    if not raw_tickets:
        return JSONResponse(content={
            'success': True,
            'total_tickets': 0,
            'all_time_stats': init_stats_dict(),
            'seven_day_data': {
                'dates': format_date_labels(end_date, 7),
                'completed': [0] * 7,
                'processing': [0] * 7,
                'failed': [0] * 7,
                'callback': [0] * 7
            }
        })
    
    # Normalize and count in memory
    normalized_tickets = normalize_ticket_list(raw_tickets)
    total_counts = count_by_status(normalized_tickets)
    daily_counts = count_by_date_and_status(normalized_tickets, end_date - timedelta(days=6), 7)
    
    # Format response
    sorted_dates = sorted(daily_counts.keys())
    seven_day_breakdown = {
        'dates': [datetime.strptime(d, '%Y-%m-%d').strftime('%b %d, %Y') for d in sorted_dates],
        'completed': [daily_counts[d]['completed'] for d in sorted_dates],
        'processing': [daily_counts[d]['processing'] for d in sorted_dates],
        'failed': [daily_counts[d]['failed'] for d in sorted_dates],
        'callback': [daily_counts[d]['callback'] for d in sorted_dates]
    }
    
    response_data = {
        'success': True,
        'total_tickets': len(normalized_tickets),
        'all_time_stats': total_counts,
        'seven_day_data': seven_day_breakdown
    }
    
    await set_cache(cache_key, response_data, expire=DEFAULT_CACHE_TTL)
    return JSONResponse(content=response_data)

@app.get("/api/ticket-data-range")
@require_db
@rate_limit
async def get_ticket_data_range(request: Request, start_date: str, end_date: str, client_ids: str = None):
    """Get ticket data for date range using only find()"""
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999)
    
    cache_key = f"range:{start_date}:{end_date}:{client_ids or 'all'}"
    cached_data = await get_cache(cache_key, endpoint="ticket_data_range")
    if cached_data:
        return JSONResponse(content=cached_data)
    
    # Fetch using only find()
    if materialized_view is not None:
        match_conditions = {"date": {"$gte": start_date, "$lte": end_date}}
        
        if client_ids:
            client_list = [c.strip() for c in client_ids.split(",")]
            match_conditions["client_id"] = {"$in": client_list}
        
        projection = {"date": 1, "client_id": 1, "status": 1, "count": 1, "_id": 0}
        
        raw_view_data = await fetch_view_raw(
            match_conditions=match_conditions,
            projection=projection,
            limit=100000,
            sort=None
        )
    else:
        match_conditions = {"created_at": {"$gte": start, "$lte": end}}
        
        if client_ids:
            client_list = [c.strip() for c in client_ids.split(",")]
            match_conditions["client_id"] = {"$in": client_list}
        
        projection = {
            "created_at": 1,
            "client_id": 1,
            "client": 1,
            "auto_qa_status": 1,
            "request_status": 1,
            "qa": 1,
            "request": 1,
            "_id": 0
        }
        
        raw_tickets = await fetch_tickets_raw(
            match_conditions=match_conditions,
            projection=projection,
            limit=100000
        )
        
        # Normalize and convert to view format
        normalized_tickets = normalize_ticket_list(raw_tickets)
        
        raw_view_data = []
        for ticket in normalized_tickets:
            raw_view_data.append({
                'date': extract_date(ticket).strftime('%Y-%m-%d'),
                'client_id': str(ticket.get('client_id', '')),
                'status': parse_ticket_status(ticket),
                'count': 1
            })
    
    # Transform all in memory
    total_stats = init_stats_dict()
    daily_data = {}
    client_breakdown = {}
    
    for entry in raw_view_data:
        date = entry.get('date')
        client_id = entry.get('client_id')
        status = entry.get('status')
        count = entry.get('count', 1)
        
        if status in total_stats:
            total_stats[status] += count
        
        if date not in daily_data:
            daily_data[date] = init_stats_dict()
        if status in daily_data[date]:
            daily_data[date][status] += count
        
        if client_id not in client_breakdown:
            client_breakdown[client_id] = {**init_stats_dict(), "total": 0}
        if status in client_breakdown[client_id]:
            client_breakdown[client_id][status] += count
            client_breakdown[client_id]["total"] += count
    
    # Format response
    dates = sorted(daily_data.keys())
    daily_breakdown = {
        "dates": [datetime.strptime(d, "%Y-%m-%d").strftime("%b %d") for d in dates] if dates else [],
        "completed": [daily_data[d]["completed"] for d in dates] if dates else [],
        "processing": [daily_data[d]["processing"] for d in dates] if dates else [],
        "failed": [daily_data[d]["failed"] for d in dates] if dates else [],
        "callback": [daily_data[d]["callback"] for d in dates] if dates else []
    }
    
    response_data = {
        "success": True,
        "total_days": (end - start).days + 1,
        "total_stats": total_stats,
        "daily_breakdown": daily_breakdown,
        "client_breakdown": client_breakdown
    }
    
    await set_cache(cache_key, response_data, expire=VIEW_CACHE_TTL)
    return JSONResponse(content=response_data)

@app.get("/api/refresh-view")
async def manual_refresh_view():
    """Manually trigger materialized view refresh"""
    try:
        await refresh_materialized_view()
        return JSONResponse(content={"status": "success", "message": "View refreshed"})
    except Exception as e:
        logger.error(f"Manual refresh failed: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/api/refresh-view-full")
async def manual_refresh_view_full():
    """Manually trigger FULL materialized view refresh"""
    try:
        duration = await refresh_materialized_view_full()
        return JSONResponse(content={
            "status": "success",
            "message": f"Full view refresh completed in {duration:.1f}s"
        })
    except Exception as e:
        logger.error(f"Full refresh failed: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/reload-data")
@require_db
async def reload_data():
    """Reload data - clear cache and refresh view"""
    await clear_cache_pattern("*")
    await refresh_materialized_view()
    
    main_count = await collection.count_documents({})
    view_count = await materialized_view.count_documents({}) if materialized_view is not None else 0
    
    return JSONResponse(content={
        "status": "success",
        "message": f"Reloaded. Main: {main_count:,} docs. View: {view_count:,} entries."
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
