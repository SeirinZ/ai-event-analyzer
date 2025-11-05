import hashlib
import json
from datetime import datetime
from config import CACHE_MAX_SIZE, CACHE_EXPIRY_SECONDS

QUERY_CACHE = {}

def get_cache_key(query, filters):
    """Generate cache key"""
    key_str = f"{query.lower().strip()}_{json.dumps(filters, sort_keys=True)}"
    return hashlib.md5(key_str.encode()).hexdigest()


def get_from_cache(cache_key):
    """Get from cache if not expired"""
    if cache_key in QUERY_CACHE:
        cached = QUERY_CACHE[cache_key]
        elapsed = (datetime.now() - cached['timestamp']).seconds
        if elapsed < CACHE_EXPIRY_SECONDS:
            return cached['data']
    return None


def save_to_cache(cache_key, data):
    """Save to cache with size limit"""
    global QUERY_CACHE
    
    if len(QUERY_CACHE) >= CACHE_MAX_SIZE:
        # Remove oldest entry
        oldest = min(QUERY_CACHE.items(), key=lambda x: x[1]['timestamp'])
        del QUERY_CACHE[oldest[0]]
    
    QUERY_CACHE[cache_key] = {
        'data': data,
        'timestamp': datetime.now()
    }


def clear_cache():
    """Clear all cache"""
    global QUERY_CACHE
    QUERY_CACHE = {}


def get_cache_stats():
    """Get cache statistics"""
    return {
        "total_cached": len(QUERY_CACHE),
        "max_size": CACHE_MAX_SIZE,
        "usage_pct": round(len(QUERY_CACHE) / CACHE_MAX_SIZE * 100, 1) if CACHE_MAX_SIZE > 0 else 0
    }