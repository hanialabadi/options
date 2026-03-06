"""
IV Collector — REST-based daily IV surface ingestion.

Modules:
    chain_surface   — flatten Schwab /chains response → constant-maturity IV surface
    contract_builder — ATM strike detection + streamer symbol construction
    rest_collector  — orchestrator: fetch surface for all tickers and write to DuckDB
"""
