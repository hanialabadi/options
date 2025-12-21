# pcs_engine_runner.py

from core.pcs_engine_v3 import run_pcs_engine_v3
from core.pcs_engine_v4 import run_pcs_engine_v4
from core.pcs_engine_v5 import run_pcs_engine_v5

def run_pcs_engine(df, version="v5"):
    if version == "v3":
        return run_pcs_engine_v3(df)
    elif version == "v4":
        return run_pcs_engine_v4(df)
    else:
        return run_pcs_engine_v5(df)
