"""
Chain Cache Infrastructure for Step 9B Scalability

Multi-level caching system:
1. In-memory LRU cache (100 tickers max)
2. Disk cache with TTL (24-48h)
3. Automatic invalidation and refresh

Purpose: Fetch option chains ONCE per ticker, cache for all strategies.
Expected improvement: 50-70% runtime reduction on subsequent runs.
"""

import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
import hashlib
import json

logger = logging.getLogger(__name__)


class ChainCache:
    """
    Multi-level option chain cache with automatic expiration.
    
    Cache hierarchy:
    1. Memory (fast, volatile)
    2. Disk (persistent, TTL-based)
    
    Usage:
        cache = ChainCache(cache_dir='data/chain_cache')
        
        # Try to get from cache
        chain = cache.get('AAPL', max_age_hours=24)
        if chain is None:
            # Fetch from API
            chain = fetch_option_chain('AAPL')
            cache.set('AAPL', chain)
        
        # Use cached chain
        process_chain(chain)
    """
    
    def __init__(self, cache_dir: str = 'data/chain_cache', max_memory_size: int = 100):
        """
        Initialize chain cache.
        
        Args:
            cache_dir: Directory for disk cache persistence
            max_memory_size: Maximum number of chains to keep in memory (LRU)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
        self.max_memory_size = max_memory_size
        self.access_order = []  # For LRU eviction
        
        # Stats tracking
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        
        logger.info(f"📦 ChainCache initialized: {self.cache_dir}")
        logger.info(f"   Max memory size: {max_memory_size} chains")
    
    def get(self, ticker: str, max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
        """
        Get chain from cache (memory → disk → None).
        
        Args:
            ticker: Stock ticker symbol
            max_age_hours: Maximum age of cached data in hours
        
        Returns:
            Cached chain data or None if not found/expired
        """
        # 1. Check memory cache
        if ticker in self.memory_cache:
            cache_entry = self.memory_cache[ticker]
            age_hours = (datetime.now() - cache_entry['cached_at']).total_seconds() / 3600
            
            if age_hours < max_age_hours:
                self.hits += 1
                self._update_access_order(ticker)
                logger.debug(f"✅ Memory cache HIT: {ticker} (age: {age_hours:.1f}h)")
                return cache_entry['data']
            else:
                # Expired in memory
                logger.debug(f"⚠️  Memory cache EXPIRED: {ticker} (age: {age_hours:.1f}h)")
                del self.memory_cache[ticker]
                self.access_order.remove(ticker)
        
        # 2. Check disk cache
        cache_file = self._get_cache_file(ticker)
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    cache_entry = pickle.load(f)
                
                age_hours = (datetime.now() - cache_entry['cached_at']).total_seconds() / 3600
                
                if age_hours < max_age_hours:
                    self.hits += 1
                    # Promote to memory cache
                    self._set_memory(ticker, cache_entry['data'], cache_entry['cached_at'])
                    logger.debug(f"✅ Disk cache HIT: {ticker} (age: {age_hours:.1f}h) - promoted to memory")
                    return cache_entry['data']
                else:
                    # Expired on disk
                    logger.debug(f"⚠️  Disk cache EXPIRED: {ticker} (age: {age_hours:.1f}h)")
                    cache_file.unlink()
            
            except Exception as e:
                logger.warning(f"⚠️  Failed to load disk cache for {ticker}: {e}")
                # Clean up corrupted cache file
                if cache_file.exists():
                    cache_file.unlink()
        
        # 3. Cache miss
        self.misses += 1
        logger.debug(f"❌ Cache MISS: {ticker}")
        return None
    
    def set(self, ticker: str, chain_data: Dict[str, Any]) -> None:
        """
        Save chain to all cache levels.
        
        Args:
            ticker: Stock ticker symbol
            chain_data: Option chain data to cache
        """
        now = datetime.now()
        
        # 1. Save to memory cache
        self._set_memory(ticker, chain_data, now)
        
        # 2. Save to disk cache
        try:
            cache_file = self._get_cache_file(ticker)
            cache_entry = {
                'ticker': ticker,
                'data': chain_data,
                'cached_at': now
            }
            
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_entry, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            logger.debug(f"💾 Cached to disk: {ticker}")
        
        except Exception as e:
            logger.warning(f"⚠️  Failed to save disk cache for {ticker}: {e}")
    
    def _set_memory(self, ticker: str, chain_data: Dict[str, Any], cached_at: datetime) -> None:
        """Set chain in memory cache with LRU eviction."""
        # Check if we need to evict
        if len(self.memory_cache) >= self.max_memory_size and ticker not in self.memory_cache:
            # Evict least recently used
            lru_ticker = self.access_order[0]
            del self.memory_cache[lru_ticker]
            self.access_order.pop(0)
            self.evictions += 1
            logger.debug(f"🗑️  Evicted from memory cache: {lru_ticker}")
        
        # Add to memory cache
        self.memory_cache[ticker] = {
            'data': chain_data,
            'cached_at': cached_at
        }
        self._update_access_order(ticker)
    
    def _update_access_order(self, ticker: str) -> None:
        """Update LRU access order."""
        if ticker in self.access_order:
            self.access_order.remove(ticker)
        self.access_order.append(ticker)
    
    def _get_cache_file(self, ticker: str) -> Path:
        """Get cache file path for ticker."""
        # Use date in filename for automatic daily invalidation
        date_str = datetime.now().strftime('%Y-%m-%d')
        return self.cache_dir / f"{ticker}_{date_str}.pkl"
    
    def invalidate(self, ticker: str) -> None:
        """Force invalidate cache for a specific ticker."""
        # Remove from memory
        if ticker in self.memory_cache:
            del self.memory_cache[ticker]
            self.access_order.remove(ticker)
        
        # Remove from disk (all dates)
        for cache_file in self.cache_dir.glob(f"{ticker}_*.pkl"):
            cache_file.unlink()
        
        logger.info(f"🗑️  Invalidated cache: {ticker}")
    
    def clear_all(self) -> None:
        """Clear all cached data."""
        self.memory_cache.clear()
        self.access_order.clear()
        
        for cache_file in self.cache_dir.glob("*.pkl"):
            cache_file.unlink()
        
        logger.info("🗑️  Cleared all cache")
    
    def cleanup_expired(self, max_age_hours: int = 48) -> int:
        """
        Clean up expired cache files from disk.
        
        Args:
            max_age_hours: Files older than this are deleted
        
        Returns:
            Number of files deleted
        """
        deleted = 0
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        for cache_file in self.cache_dir.glob("*.pkl"):
            if datetime.fromtimestamp(cache_file.stat().st_mtime) < cutoff:
                cache_file.unlink()
                deleted += 1
        
        if deleted > 0:
            logger.info(f"🗑️  Cleaned up {deleted} expired cache files")
        
        return deleted
    
    def stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache metrics
        """
        total_requests = self.hits + self.misses
        hit_rate = self.hits / total_requests if total_requests > 0 else 0.0
        
        disk_files = len(list(self.cache_dir.glob("*.pkl")))
        disk_size_mb = sum(f.stat().st_size for f in self.cache_dir.glob("*.pkl")) / (1024 * 1024)
        
        return {
            'memory_size': len(self.memory_cache),
            'memory_max': self.max_memory_size,
            'disk_files': disk_files,
            'disk_size_mb': round(disk_size_mb, 2),
            'hits': self.hits,
            'misses': self.misses,
            'evictions': self.evictions,
            'hit_rate': hit_rate,
            'total_requests': total_requests
        }
    
    def __repr__(self) -> str:
        stats = self.stats()
        return (f"ChainCache(memory={stats['memory_size']}/{stats['memory_max']}, "
                f"disk={stats['disk_files']} files, "
                f"hit_rate={stats['hit_rate']:.1%})")


class ExpirationCache:
    """
    Lightweight cache for expiration-only data.
    
    Used in Phase 1 for fast expiration metadata lookup.
    Separate from ChainCache because expiration data is much smaller.
    """
    
    def __init__(self, cache_dir: str = 'data/expiration_cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
        
        logger.info(f"📅 ExpirationCache initialized: {self.cache_dir}")
    
    def get(self, ticker: str, max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
        """Get expiration metadata from cache."""
        # Check memory
        if ticker in self.memory_cache:
            cache_entry = self.memory_cache[ticker]
            age_hours = (datetime.now() - cache_entry['cached_at']).total_seconds() / 3600
            
            if age_hours < max_age_hours:
                return cache_entry['data']
        
        # Check disk
        cache_file = self.cache_dir / f"{ticker}_{datetime.now().date()}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    cache_entry = json.load(f)
                
                cached_at = datetime.fromisoformat(cache_entry['cached_at'])
                age_hours = (datetime.now() - cached_at).total_seconds() / 3600
                
                if age_hours < max_age_hours:
                    # Promote to memory
                    self.memory_cache[ticker] = {
                        'data': cache_entry['data'],
                        'cached_at': cached_at
                    }
                    return cache_entry['data']
            
            except Exception as e:
                logger.warning(f"⚠️  Failed to load expiration cache for {ticker}: {e}")
        
        return None
    
    def set(self, ticker: str, expiration_data: Dict[str, Any]) -> None:
        """Save expiration metadata to cache."""
        now = datetime.now()
        
        # Memory
        self.memory_cache[ticker] = {
            'data': expiration_data,
            'cached_at': now
        }
        
        # Disk
        try:
            cache_file = self.cache_dir / f"{ticker}_{now.date()}.json"
            cache_entry = {
                'ticker': ticker,
                'data': expiration_data,
                'cached_at': now.isoformat()
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_entry, f, indent=2)
        
        except Exception as e:
            logger.warning(f"⚠️  Failed to save expiration cache for {ticker}: {e}")
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        disk_files = len(list(self.cache_dir.glob("*.json")))
        
        return {
            'memory_size': len(self.memory_cache),
            'disk_files': disk_files
        }
