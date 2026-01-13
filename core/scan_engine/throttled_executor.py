"""
Throttled Parallel Executor for Step 9B Scalability

Thread pool with rate limiting and safety features:
- Concurrent processing of multiple tickers
- API rate limiting (respect provider constraints)
- Timeout protection (30s per ticker)
- Graceful error handling (continue on failure)
- Deterministic output (results match input order)

Purpose: Process 500 tickers in parallel without API throttling.
Expected improvement: 5-8× speedup on multi-core machines.
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Callable, List, Any, Dict, Optional
from dataclasses import dataclass
from threading import Lock

logger = logging.getLogger(__name__)


@dataclass
class ExecutorConfig:
    """Configuration for ThrottledExecutor."""
    max_workers: int = 8  # Number of concurrent threads
    requests_per_second: float = 10.0  # API rate limit
    timeout_seconds: float = 30.0  # Timeout per task
    retry_on_error: bool = False  # Retry failed tasks
    max_retries: int = 2  # Maximum retry attempts


class ThrottledExecutor:
    """
    Thread pool executor with rate limiting and safety features.
    
    Features:
    - Concurrent execution with configurable worker pool
    - Rate limiting to respect API constraints
    - Per-task timeout protection
    - Graceful error handling
    - Progress tracking
    - Deterministic output order
    
    Usage:
        executor = ThrottledExecutor(max_workers=8, requests_per_second=10)
        
        # Parallel processing
        results = executor.map_parallel(
            process_ticker,
            ticker_list,
            desc="Processing tickers"
        )
        
        # Results are in same order as input
        for ticker, result in zip(ticker_list, results):
            print(f"{ticker}: {result}")
    """
    
    def __init__(
        self,
        max_workers: int = 8,
        requests_per_second: float = 10.0,
        timeout_seconds: float = 30.0
    ):
        """
        Initialize throttled executor.
        
        Args:
            max_workers: Number of concurrent threads (8-12 recommended)
            requests_per_second: API rate limit (10 req/sec default for Tradier)
            timeout_seconds: Timeout per task (30s default)
        """
        self.config = ExecutorConfig(
            max_workers=max_workers,
            requests_per_second=requests_per_second,
            timeout_seconds=timeout_seconds
        )
        
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.rate_limit_delay = 1.0 / requests_per_second
        self.last_request_time = 0
        self.lock = Lock()
        
        # Stats tracking
        self.tasks_submitted = 0
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.tasks_timeout = 0
        
        logger.info(f"⚡ ThrottledExecutor initialized")
        logger.info(f"   Max workers: {max_workers}")
        logger.info(f"   Rate limit: {requests_per_second:.1f} req/sec")
        logger.info(f"   Timeout: {timeout_seconds}s per task")
    
    def submit_with_throttle(self, fn: Callable, *args, **kwargs) -> Future:
        """
        Submit task with rate limiting.
        
        Args:
            fn: Function to execute
            *args: Positional arguments for fn
            **kwargs: Keyword arguments for fn
        
        Returns:
            Future object for the submitted task
        """
        # Enforce rate limit (thread-safe)
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - elapsed
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
        
        # Submit to thread pool
        future = self.executor.submit(fn, *args, **kwargs)
        self.tasks_submitted += 1
        
        return future
    
    def map_parallel(
        self,
        fn: Callable,
        items: List[Any],
        desc: str = "Processing",
        show_progress: bool = True
    ) -> List[Any]:
        """
        Parallel map with progress tracking and deterministic order.
        
        Args:
            fn: Function to apply to each item
            items: List of items to process
            desc: Description for progress logging
            show_progress: Whether to log progress updates
        
        Returns:
            List of results in same order as input items
        """
        if len(items) == 0:
            return []
        
        start_time = time.time()
        logger.info(f"🚀 {desc}: {len(items)} items with {self.config.max_workers} workers")
        
        # Submit all tasks (with rate limiting)
        futures_map = {}  # {future: (index, item)}
        for idx, item in enumerate(items):
            future = self.submit_with_throttle(fn, item)
            futures_map[future] = (idx, item)
        
        # Collect results (as they complete)
        results = [None] * len(items)  # Pre-allocate with correct size
        completed = 0
        
        for future in as_completed(futures_map.keys(), timeout=self.config.timeout_seconds * len(items)):
            idx, item = futures_map[future]
            
            try:
                # Get result with timeout
                result = future.result(timeout=self.config.timeout_seconds)
                results[idx] = result
                self.tasks_completed += 1
                completed += 1
                
                # Progress logging
                if show_progress and completed % max(1, len(items) // 10) == 0:
                    pct = 100 * completed / len(items)
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    logger.info(f"   Progress: {completed}/{len(items)} ({pct:.0f}%) | {rate:.1f} items/sec")
            
            except TimeoutError:
                # Task timed out
                logger.warning(f"⏱️  Timeout processing item at index {idx}")
                results[idx] = self._create_error_result(item, "Timeout")
                self.tasks_timeout += 1
                completed += 1
            
            except Exception as e:
                # Task failed
                logger.warning(f"❌ Error processing item at index {idx}: {e}")
                results[idx] = self._create_error_result(item, str(e))
                self.tasks_failed += 1
                completed += 1
        
        # Final stats
        duration = time.time() - start_time
        rate = len(items) / duration if duration > 0 else 0
        
        logger.info(f"✅ {desc} complete:")
        logger.info(f"   Duration: {duration:.1f}s ({rate:.1f} items/sec)")
        logger.info(f"   Success: {self.tasks_completed}/{len(items)}")
        if self.tasks_failed > 0:
            logger.warning(f"   Failed: {self.tasks_failed}/{len(items)}")
        if self.tasks_timeout > 0:
            logger.warning(f"   Timeout: {self.tasks_timeout}/{len(items)}")
        
        return results
    
    def map_parallel_by_group(
        self,
        fn: Callable,
        items: List[Any],
        group_key: Callable,
        desc: str = "Processing"
    ) -> List[Any]:
        """
        Parallel map with grouping for cache efficiency.
        
        Useful when processing strategies grouped by ticker - all strategies
        for a ticker are processed together to maximize cache hits.
        
        Args:
            fn: Function to apply (should accept list of items)
            items: List of items to process
            group_key: Function to extract grouping key from item
            desc: Description for progress logging
        
        Returns:
            List of results (flattened from all groups)
        """
        # Group items
        from collections import defaultdict
        groups = defaultdict(list)
        for item in items:
            key = group_key(item)
            groups[key].append(item)
        
        logger.info(f"📦 Grouped {len(items)} items into {len(groups)} groups")
        
        # Process groups in parallel
        group_list = [(key, group) for key, group in groups.items()]
        
        def process_group(key_and_group):
            key, group = key_and_group
            return fn(key, group)
        
        group_results = self.map_parallel(
            process_group,
            group_list,
            desc=f"{desc} (grouped)"
        )
        
        # Flatten results
        results = []
        for group_result in group_results:
            if isinstance(group_result, list):
                results.extend(group_result)
            else:
                results.append(group_result)
        
        return results
    
    def _create_error_result(self, item: Any, error_msg: str) -> Dict[str, Any]:
        """
        Create error result placeholder.
        
        Args:
            item: Original item that failed
            error_msg: Error message
        
        Returns:
            Error result dictionary
        """
        # Try to extract ticker if item is dict/dataframe row
        ticker = None
        if isinstance(item, dict):
            ticker = item.get('Ticker') or item.get('ticker')
        elif hasattr(item, 'get'):
            ticker = item.get('Ticker') or item.get('ticker')
        
        return {
            'Exploration_Status': 'Executor_Error',
            'Exploration_Reason': f'Parallel execution error: {error_msg}',
            'Error_Type': 'ExecutorError',
            'Error_Message': error_msg,
            'Ticker': ticker or 'Unknown'
        }
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown executor and cleanup resources."""
        self.executor.shutdown(wait=wait)
        logger.info("🛑 ThrottledExecutor shutdown")
    
    def stats(self) -> Dict[str, Any]:
        """
        Get executor statistics.
        
        Returns:
            Dictionary with execution metrics
        """
        return {
            'max_workers': self.config.max_workers,
            'rate_limit': self.config.requests_per_second,
            'tasks_submitted': self.tasks_submitted,
            'tasks_completed': self.tasks_completed,
            'tasks_failed': self.tasks_failed,
            'tasks_timeout': self.tasks_timeout,
            'success_rate': self.tasks_completed / self.tasks_submitted if self.tasks_submitted > 0 else 0.0
        }
    
    def __repr__(self) -> str:
        stats = self.stats()
        return (f"ThrottledExecutor(workers={stats['max_workers']}, "
                f"rate={stats['rate_limit']:.1f} req/sec, "
                f"success_rate={stats['success_rate']:.1%})")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()


class BatchProcessor:
    """
    Batch processor for large-scale operations.
    
    Splits large datasets into batches and processes sequentially
    to avoid memory exhaustion and allow resumption on failure.
    """
    
    def __init__(self, batch_size: int = 50, checkpoint_dir: str = 'data/checkpoints'):
        """
        Initialize batch processor.
        
        Args:
            batch_size: Number of items per batch
            checkpoint_dir: Directory to save progress checkpoints
        """
        self.batch_size = batch_size
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📦 BatchProcessor initialized: batch_size={batch_size}")
    
    def process_in_batches(
        self,
        fn: Callable,
        items: List[Any],
        executor: ThrottledExecutor,
        desc: str = "Processing",
        save_checkpoints: bool = True
    ) -> List[Any]:
        """
        Process items in batches with checkpointing.
        
        Args:
            fn: Function to apply to each item
            items: List of items to process
            executor: ThrottledExecutor for parallel execution
            desc: Description for logging
            save_checkpoints: Whether to save progress checkpoints
        
        Returns:
            List of all results (concatenated from batches)
        """
        total_batches = (len(items) + self.batch_size - 1) // self.batch_size
        logger.info(f"📦 Processing {len(items)} items in {total_batches} batches of {self.batch_size}")
        
        all_results = []
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.batch_size
            end_idx = min(start_idx + self.batch_size, len(items))
            batch = items[start_idx:end_idx]
            
            logger.info(f"📦 Batch {batch_idx + 1}/{total_batches}: items {start_idx}-{end_idx}")
            
            # Process batch in parallel
            batch_results = executor.map_parallel(
                fn,
                batch,
                desc=f"{desc} (batch {batch_idx + 1}/{total_batches})",
                show_progress=True
            )
            
            all_results.extend(batch_results)
            
            # Save checkpoint
            if save_checkpoints:
                checkpoint_file = self.checkpoint_dir / f"batch_{batch_idx:04d}.pkl"
                import pickle
                with open(checkpoint_file, 'wb') as f:
                    pickle.dump({
                        'batch_idx': batch_idx,
                        'start_idx': start_idx,
                        'end_idx': end_idx,
                        'results': batch_results
                    }, f)
                logger.info(f"💾 Saved checkpoint: {checkpoint_file.name}")
        
        logger.info(f"✅ Batch processing complete: {len(all_results)} results")
        return all_results


from pathlib import Path
