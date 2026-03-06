"""
Unified Pipeline Runner

Provides environment-agnostic execution for Scan and Management pipelines.
Supports log streaming and async execution for UI integration.
"""

import subprocess
import sys
import logging
import threading
from pathlib import Path
from typing import List, Callable, Optional

logger = logging.getLogger(__name__)

class PipelineRunner:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.python_exe = sys.executable

    def run_command(self, cmd: List[str], on_log: Optional[Callable[[str], None]] = None) -> int:
        """
        Execute a command and optionally stream logs.
        """
        logger.info(f"Executing: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=sys.stdout,  # Direct stdout to parent's stdout
            stderr=sys.stderr,  # Direct stderr to parent's stderr
            text=True,
            bufsize=1,
            cwd=str(self.project_root)
        )
        
        process.wait()
        return process.returncode

    def run_management_pipeline(self, input_csv: str, on_log: Optional[Callable[[str], None]] = None):
        cmd = [self.python_exe, "-m", "core.management.run_all", "--input", input_csv]
        return self.run_command(cmd, on_log)

    def run_scan_pipeline(self, snapshot_path: str, balance: float, risk: float, sizing: str, debug: bool = False, intraday: bool = True, on_log: Optional[Callable[[str], None]] = None):
        cmd = [
            self.python_exe, "-m", "scan_engine",
            "--snapshot", snapshot_path,
            "--balance", str(balance),
            "--risk", str(risk),
            "--sizing", sizing
        ]
        if debug:
            cmd.append("--debug")
        if not intraday:
            cmd.append("--no-intraday")
        return self.run_command(cmd, on_log)
