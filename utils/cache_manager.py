"""
Cache management for query results
"""
import hashlib
import json
from datetime import datetime, timedelta
from config import CACHE_MAX_SIZE, CACHE_EXPIRY_SECONDS

class CacheManager:
    """Manages query result caching"""
    
    def __init__(self, max_size=CACHE_MAX_SIZE, expiry_seconds=CACHE_EXPIRY_SECONDS):
        self.cache = {}
        self.max_size = max_size
        self.expiry_seconds = expiry_seconds
    
    def _get_cache_key(self, query, filters):
        """Generate cache key from query and filters"""
        key_str = f"{query.lower().strip()}_{json.dumps(filters, sort_keys=True)}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, query, filters=None):
        """Get cached result if exists and not expired"""
        if filters is None:
            filters = {}
        
        cache_key = self._get_cache_key(query, filters)
        
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            # Check if expired
            if (datetime.now() - cached['timestamp']).seconds < self.expiry_seconds:
                return cached['data']
            else:
                # Remove expired entry
                del self.cache[cache_key]
        
        return None
    
    def set(self, query, filters, data):
        """Save result to cache with size limit"""
        if filters is None:
            filters = {}
        
        cache_key = self._get_cache_key(query, filters)
        
        # Remove oldest if at capacity
        if len(self.cache) >= self.max_size:
            oldest = min(self.cache.items(), key=lambda x: x[1]['timestamp'])
            del self.cache[oldest[0]]
        
        self.cache[cache_key] = {
            'data': data,
            'timestamp': datetime.now()
        }
    
    def clear(self):
        """Clear all cache"""
        self.cache = {}
    
    def stats(self):
        """Get cache statistics"""
        return {
            'total_cached': len(self.cache),
            'max_size': self.max_size,
            'usage_pct': round(len(self.cache) / self.max_size * 100, 1) if self.max_size > 0 else 0
        }