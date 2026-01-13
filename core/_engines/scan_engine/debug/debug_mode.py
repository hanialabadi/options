import os
import traceback
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional

class DebugEvent:
    def __init__(self, step: str, severity: str, code: str, message: str, context: Dict[str, Any] = None):
        self.timestamp = datetime.now()
        self.step = step
        self.severity = severity
        self.code = code
        self.message = message
        self.context = context or {}

    def to_dict(self):
        return {
            "timestamp": self.timestamp.strftime("%H:%M:%S.%f")[:-3],
            "step": self.step,
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "context": self.context
        }

class DebugManager:
    def __init__(self):
        # Single source of truth: Environment variable only
        self.enabled = os.getenv("PIPELINE_DEBUG") == "1"
        self.events: List[DebugEvent] = []
        self.step_counts: Dict[str, int] = {}
        self.artifacts: Dict[str, pd.DataFrame] = {}
        # Deterministic debug universe (RAG-aligned)
        self.debug_tickers = ["AAPL", "AMZN", "NVDA"]

    def restrict_universe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Restricts the ticker universe if debug mode is enabled.
        """
        if not self.enabled:
            return df

        import logging
        logger = logging.getLogger(__name__)
        
        # Identify ID column (Symbol or Ticker)
        id_col = 'Symbol' if 'Symbol' in df.columns else 'Ticker'
        if id_col in df.columns:
            logger.info(f"🧪 TEST MODE ACTIVE (PIPELINE_DEBUG=1) — restricting universe to {self.debug_tickers}")
            df_restricted = df[df[id_col].isin(self.debug_tickers)].copy()
            return df_restricted
        else:
            logger.warning("⚠️ TEST MODE ACTIVE but could not find ID column for filtering")
            return df

    def restrict_ticker_list(self, tickers: List[str]) -> List[str]:
        """
        Restricts a list of tickers if debug mode is enabled.
        """
        if not self.enabled:
            return tickers

        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"🧪 TEST MODE ACTIVE (PIPELINE_DEBUG=1) — restricting universe to {self.debug_tickers}")
        return [t for t in tickers if t in self.debug_tickers]

    def log_event(self, step: str, severity: str, code: str, message: str, context: Dict[str, Any] = None):
        if not self.enabled:
            return
        event = DebugEvent(step, severity, code, message, context)
        self.events.append(event)

    def log_exception(self, step: str, exception: Exception, recovery_action: str, context: Dict[str, Any] = None):
        if not self.enabled:
            return
        ctx = context or {}
        ctx.update({
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
            "traceback": traceback.format_exc(),
            "recovery_action": recovery_action
        })
        self.log_event(step, "ERROR", "EXCEPTION_CAUGHT", f"Swallowed exception: {str(exception)}", ctx)

    def record_step(self, step: str, count: int, df: Optional[pd.DataFrame] = None):
        if not self.enabled:
            return
        self.step_counts[step] = count
        if df is not None:
            # Store a copy of the dataframe for inspection
            self.artifacts[step] = df.copy()

    def get_summary(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "step_counts": self.step_counts,
            "events": [e.to_dict() for e in self.events],
            "artifacts": {k: v.shape for k, v in self.artifacts.items()}
        }

    def clear(self):
        self.events = []
        self.step_counts = {}
        self.artifacts = {}

# Global instance for easy access if needed, though passing it through the pipeline is preferred
_global_debug_manager = DebugManager()

def get_debug_manager() -> DebugManager:
    return _global_debug_manager
