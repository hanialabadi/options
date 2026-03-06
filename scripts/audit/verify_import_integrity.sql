-- 1. Snapshot growth over time
-- Proves that re-importing appends new snapshots rather than overwriting.
SELECT 
    run_id, 
    Snapshot_TS, 
    COUNT(*) as row_count 
FROM clean_legs 
GROUP BY run_id, Snapshot_TS 
ORDER BY Snapshot_TS DESC;

-- 2. No TradeID anchor rewrites (Proves immutability)
-- Every row for a TradeID must have the same Entry_Snapshot_TS.
-- If this returns any rows, the invariant has been violated.
-- RAG: Identity Hygiene. Filter for LegID IS NOT NULL.
SELECT 
    TradeID, 
    COUNT(DISTINCT Entry_Snapshot_TS) as anchor_count 
FROM clean_legs 
WHERE LegID IS NOT NULL
GROUP BY TradeID 
HAVING anchor_count > 1;

-- 3. Drift is computed against earliest Entry_Snapshot_TS
-- Proves that Entry_Snapshot_TS matches the absolute MIN(Snapshot_TS) for that trade.
-- If this returns any rows, the anchoring logic is incorrect.
-- RAG: Identity Hygiene. Filter for LegID IS NOT NULL.
WITH trade_origins AS (
    SELECT 
        TradeID, 
        MIN(Snapshot_TS) as absolute_origin 
    FROM clean_legs 
    WHERE LegID IS NOT NULL
    GROUP BY TradeID
)
SELECT 
    c.TradeID, 
    c.Entry_Snapshot_TS, 
    o.absolute_origin,
    CASE WHEN c.Entry_Snapshot_TS = o.absolute_origin THEN '✅ CORRECT' ELSE '❌ CORRUPT' END as status
FROM clean_legs c
JOIN trade_origins o ON c.TradeID = o.TradeID
WHERE c.Entry_Snapshot_TS != o.absolute_origin
AND c.LegID IS NOT NULL
LIMIT 10;

-- 4. Canonical Anchor Integrity
-- Proves that canonical_anchors view contains exactly one row per TradeID+LegID.
SELECT 
    TradeID, 
    LegID, 
    COUNT(*) as row_count
FROM canonical_anchors
GROUP BY TradeID, LegID
HAVING row_count > 1;
