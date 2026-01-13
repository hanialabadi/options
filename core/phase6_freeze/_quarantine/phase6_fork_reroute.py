import duckdb
import pandas as pd

def read_latest_flat_legs(db_path: str = "data/pipeline.duckdb") -> pd.DataFrame:
    con = duckdb.connect(db_path)
    run_ids = con.execute("SELECT DISTINCT run_id FROM clean_legs ORDER BY run_id DESC").df()
    latest_run_id = run_ids["run_id"].iloc[0]
    df_flat = con.execute("SELECT * FROM clean_legs WHERE run_id = ?", [latest_run_id]).df()
    con.close()
    return df_flat

def read_master_active(db_path: str = "data/pipeline.duckdb") -> pd.DataFrame:
    con = duckdb.connect(db_path)
    df_master = con.execute("SELECT * FROM master_active").df()
    con.close()
    return df_master

def phase6_fork_reroute(
    db_path: str = "data/pipeline.duckdb"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Read latest data from DB
    df_flat = read_latest_flat_legs(db_path)
    df_master = read_master_active(db_path)
    df_flat = df_flat.copy()

    master_tradeids = set(df_master["TradeID"].unique())
    current_tradeids = set(df_flat["TradeID"].unique())

    # Tagging
    df_flat["IsNewTrade"] = ~df_flat["TradeID"].isin(master_tradeids)
    df_flat["IsExisting"] = df_flat["TradeID"].isin(master_tradeids)

    # Closed trades (present in master, missing from current)
    closed_tradeids = master_tradeids - current_tradeids
    df_closed = df_master[df_master["TradeID"].isin(closed_tradeids)].copy()
    df_closed["IsClosed"] = True

    print("Current snapshot (tagged):")
    print(df_flat[["TradeID", "IsNewTrade", "IsExisting"]].drop_duplicates())
    print("Closed/Archived trades:", closed_tradeids)

    return df_flat, df_closed
