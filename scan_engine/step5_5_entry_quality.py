"""
Step 5.5: Entry Quality Validation

PURPOSE:
    Validate entry timing quality to prevent chasing extended moves.
    Expert traders don't chase - they wait for pullbacks and quality entries.

DESIGN PRINCIPLE:
    - Check intraday extension (if up >5%, flag as chasing)
    - Check distance from moving averages (>5% from 50 MA = overextended)
    - Validate volume confirmation (breakout needs 1.5x+ avg volume)
    - Check momentum alignment (trend and momentum should agree)
    - Pattern completion validation (early patterns <50% = wait)

RATIONALE:
    Most retail traders lose money by chasing extended moves.
    This gate enforces the discipline human experts use.

INPUTS (from Step 5 - Chart Signals):
    - intraday_change_pct: Today's price change %
    - last_price, ma_50, ma_200: Price and moving averages
    - volume, avg_volume: Current and average volume
    - trend_direction, momentum_direction: Chart analysis
    - pattern_completion_pct (optional): Pattern maturity

OUTPUTS (added columns):
    - Entry_Quality: 'EXCELLENT' | 'GOOD' | 'FAIR' | 'CHASING'
    - Entry_Quality_Score: 0-100
    - Entry_Flags: List of quality issues
    - Entry_Recommendation: 'ENTER_NOW' | 'WAIT_PULLBACK' | 'AVOID'

INTEGRATION:
    Called between Step 5 (chart signals) and Step 7 (strategy recommendation).
    Filters out CHASING entries before strategy nomination.

Updated: 2026-02-03 (Execution Readiness - Phase 1)
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


# Entry quality thresholds
INTRADAY_EXTENSION_THRESHOLD = 5.0  # >5% intraday = chasing
MA_DISTANCE_THRESHOLD = 5.0          # >5% from 50 MA = overextended
VOLUME_CONFIRMATION_MIN = 1.5        # Need 1.5x average volume for breakouts
PATTERN_COMPLETION_MIN = 50.0        # <50% pattern complete = early entry


def validate_entry_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate entry quality for all tickers in DataFrame.

    Args:
        df: DataFrame from Step 5 with chart signals

    Returns:
        DataFrame with Entry_Quality, Entry_Quality_Score, Entry_Flags columns
    """

    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 5.5")
        return df

    logger.info(f"🎯 Step 5.5: Entry Quality Validation for {len(df)} tickers")

    # Initialize columns
    df['Entry_Quality_Score'] = 100.0
    df['Entry_Flags'] = ''
    df['Entry_Quality'] = 'GOOD'
    df['Entry_Recommendation'] = 'ENTER_NOW'

    # Process each row
    for idx, row in df.iterrows():
        score, flags, quality, recommendation = _evaluate_entry_quality(row)

        df.at[idx, 'Entry_Quality_Score'] = score
        df.at[idx, 'Entry_Flags'] = ' | '.join(flags) if flags else 'None'
        df.at[idx, 'Entry_Quality'] = quality
        df.at[idx, 'Entry_Recommendation'] = recommendation

    # Log summary
    _log_entry_quality_summary(df)

    return df


def _evaluate_entry_quality(row: pd.Series) -> Tuple[float, List[str], str, str]:
    """
    Evaluate entry quality for a single ticker.

    Returns:
        (score, flags, quality, recommendation)
    """

    score = 100.0
    flags = []

    # 1. Intraday extension check (CRITICAL - prevents chasing)
    intraday_change = row.get('intraday_change_pct', 0)
    if pd.notna(intraday_change):
        abs_change = abs(intraday_change)
        if abs_change > INTRADAY_EXTENSION_THRESHOLD:
            penalty = (abs_change - INTRADAY_EXTENSION_THRESHOLD) * 6.0  # -6 pts per % over threshold
            score -= penalty
            flags.append(f'Extended {intraday_change:+.1f}% today (chasing)')

    # 2. Distance from 50 MA (overextension check)
    last_price = row.get('last_price')
    ma_50 = row.get('ma_50') or row.get('MA_50')

    if pd.notna(last_price) and pd.notna(ma_50) and ma_50 > 0:
        dist_from_ma = ((last_price - ma_50) / ma_50) * 100
        abs_dist = abs(dist_from_ma)

        if abs_dist > MA_DISTANCE_THRESHOLD:
            penalty = (abs_dist - MA_DISTANCE_THRESHOLD) * 4.0  # -4 pts per % over threshold
            score -= penalty
            flags.append(f'Overextended {dist_from_ma:+.1f}% from 50 MA')

    # 3. Volume confirmation (for breakouts)
    volume = row.get('volume') or row.get('Volume')
    avg_volume = row.get('avg_volume') or row.get('Average_Volume')
    signal_strength = row.get('Signal_Strength', '')

    if pd.notna(volume) and pd.notna(avg_volume) and avg_volume > 0:
        volume_ratio = volume / avg_volume

        # Check for breakouts with weak volume
        if 'Bullish' in str(signal_strength) or 'Bearish' in str(signal_strength):
            if volume_ratio < VOLUME_CONFIRMATION_MIN:
                penalty = (VOLUME_CONFIRMATION_MIN - volume_ratio) * 15.0  # Up to -15 pts
                score -= penalty
                flags.append(f'Weak volume ({volume_ratio:.1f}x avg, need {VOLUME_CONFIRMATION_MIN}x)')

    # 4. Momentum alignment (trend and momentum should agree)
    trend_direction = row.get('trend_direction', '').lower()
    momentum_direction = row.get('momentum_direction', '').lower()

    if trend_direction and momentum_direction:
        if trend_direction != momentum_direction and trend_direction != 'neutral' and momentum_direction != 'neutral':
            score -= 20.0
            flags.append(f'Trend ({trend_direction}) / Momentum ({momentum_direction}) divergence')

    # 5. Pattern completion check (if available)
    pattern_completion = row.get('pattern_completion_pct')

    if pd.notna(pattern_completion):
        if pattern_completion < PATTERN_COMPLETION_MIN:
            penalty = (PATTERN_COMPLETION_MIN - pattern_completion) * 0.5  # Up to -25 pts
            score -= penalty
            flags.append(f'Pattern early ({pattern_completion:.0f}% complete, ideal >50%)')

    # 6. Gap analysis (large gaps = risky entry)
    gap_tag = row.get('gap_tag', '')

    if 'GAP_UP' in str(gap_tag) or 'GAP_DOWN' in str(gap_tag):
        # Gap + extension = very risky
        if abs(intraday_change) > 3.0:
            score -= 15.0
            flags.append(f'Gap with intraday extension (high risk)')

    # 7. Entry timing context (from chart signals)
    entry_timing = row.get('entry_timing_context', '')

    if 'LATE' in str(entry_timing):
        score -= 10.0
        flags.append('Late in pattern (reduced edge)')

    # Floor score at 0
    score = max(0.0, score)

    # Classify quality
    if score >= 80:
        quality = 'EXCELLENT'
        recommendation = 'ENTER_NOW'
    elif score >= 65:
        quality = 'GOOD'
        recommendation = 'ENTER_NOW'
    elif score >= 45:
        quality = 'FAIR'
        recommendation = 'WAIT_PULLBACK'
    else:
        quality = 'CHASING'
        recommendation = 'AVOID'

    return score, flags, quality, recommendation


def _log_entry_quality_summary(df: pd.DataFrame):
    """Log summary of entry quality validation."""

    quality_counts = df['Entry_Quality'].value_counts().to_dict()
    total = len(df)

    excellent = quality_counts.get('EXCELLENT', 0)
    good = quality_counts.get('GOOD', 0)
    fair = quality_counts.get('FAIR', 0)
    chasing = quality_counts.get('CHASING', 0)

    logger.info(f"\n📊 Step 5.5 Entry Quality Summary:")
    logger.info(f"   ✅ Excellent: {excellent}/{total} ({excellent/total*100:.1f}%)")
    logger.info(f"   ✅ Good: {good}/{total} ({good/total*100:.1f}%)")
    logger.info(f"   ⚠️  Fair: {fair}/{total} ({fair/total*100:.1f}%) - Consider waiting for pullback")
    logger.info(f"   ❌ Chasing: {chasing}/{total} ({chasing/total*100:.1f}%) - Avoid entry")

    # Log average score
    avg_score = df['Entry_Quality_Score'].mean()
    logger.info(f"   Avg Entry Quality Score: {avg_score:.1f}/100")

    # Log top quality issues
    if chasing > 0 or fair > 0:
        all_flags = df[df['Entry_Quality'].isin(['FAIR', 'CHASING'])]['Entry_Flags']
        flag_list = []
        for flags_str in all_flags:
            if flags_str and flags_str != 'None':
                flag_list.extend(flags_str.split(' | '))

        if flag_list:
            from collections import Counter
            top_issues = Counter(flag_list).most_common(3)
            logger.info(f"\n   Top Entry Quality Issues:")
            for issue, count in top_issues:
                logger.info(f"     • {issue}: {count} occurrences")


def filter_quality_entries(
    df: pd.DataFrame,
    min_quality_score: float = 65.0,
    allow_fair: bool = False
) -> pd.DataFrame:
    """
    Filter DataFrame to only quality entries (not chasing).

    Args:
        df: DataFrame with Entry_Quality columns
        min_quality_score: Minimum score to pass (default 65 = GOOD+)
        allow_fair: If True, allow FAIR entries (wait for pullback)

    Returns:
        Filtered DataFrame with only quality entries
    """

    initial_count = len(df)

    # Filter by score
    df_filtered = df[df['Entry_Quality_Score'] >= min_quality_score].copy()

    # Additional quality filter (exclude CHASING)
    if not allow_fair:
        df_filtered = df_filtered[df_filtered['Entry_Quality'].isin(['EXCELLENT', 'GOOD'])].copy()
    else:
        df_filtered = df_filtered[df_filtered['Entry_Quality'] != 'CHASING'].copy()

    filtered_count = len(df_filtered)
    removed_count = initial_count - filtered_count

    logger.info(f"🔍 Entry Quality Filter: {filtered_count}/{initial_count} passed ({removed_count} removed as chasing/poor)")

    return df_filtered


def get_entry_quality_metrics(df: pd.DataFrame) -> Dict:
    """
    Get entry quality metrics for monitoring/analysis.

    Returns:
        Dictionary with quality distribution metrics
    """

    metrics = {
        'total_tickers': len(df),
        'excellent_count': (df['Entry_Quality'] == 'EXCELLENT').sum(),
        'good_count': (df['Entry_Quality'] == 'GOOD').sum(),
        'fair_count': (df['Entry_Quality'] == 'FAIR').sum(),
        'chasing_count': (df['Entry_Quality'] == 'CHASING').sum(),
        'avg_score': df['Entry_Quality_Score'].mean(),
        'median_score': df['Entry_Quality_Score'].median(),
        'quality_entries_pct': ((df['Entry_Quality'].isin(['EXCELLENT', 'GOOD'])).sum() / len(df) * 100) if len(df) > 0 else 0
    }

    return metrics
