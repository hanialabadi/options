# RAG Realignment Plan: Contract-First Documentation

## Current State Audit
The current RAG documentation describes *implementation details* (how the code works) rather than *system guarantees* (what the system knows). This leads to LLM hallucinations regarding data readiness and fallback reliability.

## Realignment Principles
1.  **Contract-First**: RAG must index `core/governance/contracts.py` as the source of truth for data readiness.
2.  **Explicit Readiness**: Document "Warm-up" vs "Ready" states for IV Rank.
3.  **Provenance Governance**: Document the hierarchy of data sources (Local > VIC > Neutral).

## Phase Truth Statements (Deliverables)

### Phase 1: Snapshot Integrity
*   **Guarantee**: Every row has a valid `Ticker`, `Price`, and `HV`.
*   **Hard Gate**: Pipeline aborts if schema is non-normalized.

### Phase 2: Volatility Surface
*   **Guarantee**: `IV_Rank_Source` is explicitly tagged.
*   **Truth**: `LOCAL` source means 120+ days of history. `VIC_FALLBACK` means external proxy. `NEUTRAL` means default 50.0.

### Phase 3: Technical Indicators
*   **Guarantee**: `Signal_Type` is NA-safe and constrained to `['Bullish', 'Bearish', 'Bidirectional']`.
*   **Truth**: No "Unknown" signals allowed to propagate to strategy discovery.

### Phase 5: Strategy Discovery
*   **Guarantee**: Append-only ledger. Discovery does not filter tickers.
*   **Truth**: Multiple strategies per ticker are historical facts, not execution intents.

## Implementation Roadmap
1.  **Phase Truth Pages**: Create one-page Markdown files for each phase in `core/governance/docs/`.
2.  **Contract Indexing**: Update the RAG ingestion pipeline to prioritize `governance/` over `legacy/` or `archive/`.
3.  **Hallucination Guard**: Instruct the LLM to check `IV_Rank_Source` before reasoning about PCS confidence.
