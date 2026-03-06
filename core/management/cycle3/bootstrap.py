import logging
import os
from core.management.cycle3.decision.engine import DoctrineAuthority

logger = logging.getLogger(__name__)

def bootstrap_doctrines():
    """
    Unconditional bootstrap for Cycle 3 doctrines.
    Guarantees that all strategy-specific logic is registered before evaluation.
    """
    # RAG: Execution Path Verification
    logger.info(f"DOCTRINE BOOTSTRAP START | pid={os.getpid()} | authority_id={id(DoctrineAuthority)}")
    
    # Registration logic
    doctrines = [
        'BUY_WRITE', 'COVERED_CALL', 'BUY_CALL', 'BUY_PUT', 'CSP', 'STRADDLE', 'STRANGLE'
    ]
    DoctrineAuthority._REGISTERED_DOCTRINES = doctrines
    
    logger.info(f"DOCTRINE BOOTSTRAP COMPLETE | pid={os.getpid()} | authority_id={id(DoctrineAuthority)} | keys={DoctrineAuthority._REGISTERED_DOCTRINES}")

# Execute on import to ensure registration in any process that imports this module
bootstrap_doctrines()
