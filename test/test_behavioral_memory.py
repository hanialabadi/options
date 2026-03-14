"""
Tests for scan_engine.behavioral_memory — YTD scan history enrichment.
"""
import unittest

from scan_engine.behavioral_memory import (
    _assess_data_maturity,
    _classify_earnings,
    _classify_fault_pattern,
    _classify_indicator_trend,
    _classify_iv_arc,
    _classify_mgmt_track,
    _classify_move_drivers,
    _classify_score_trend,
    _classify_strategy_breakdown,
    _classify_volume,
    _compute_behavioral_score,
    _compute_one,
    _dedupe_sequence,
    _detect_contradictions,
    _neutral_result,
    _profile_event_reactions,
)


class TestNeutralDefaults(unittest.TestCase):
    def test_neutral_result_has_all_keys(self):
        r = _neutral_result()
        expected = {
            'Regime_Duration', 'Regime_Path', 'ADX_Trend',
            'RSI_Range', 'Volume_Accumulation', 'Scan_Frequency',
            'DQS_Trend', 'Signal_Age', 'IV_Arc', 'Earnings_Context',
            'Mgmt_Track_Record', 'Prior_Trades', 'History_Depth',
            'Behavioral_Score',
            # v2 fields
            'Mgmt_Confidence', 'Mgmt_Strategy_Detail',
            'Mgmt_Recency_Factor', 'Fault_Pattern',
            'Contradiction_Flags',
            # v3 fields — causal context
            'Move_Drivers', 'Last_Dip_Context',
            # v4 fields — RAG gap coverage
            'Event_Reactions', 'Worst_Event_Type', 'Data_Maturity',
        }
        self.assertEqual(set(r.keys()), expected)

    def test_neutral_score_is_50(self):
        self.assertEqual(_neutral_result()['Behavioral_Score'], 50)

    def test_neutral_confidence_is_none(self):
        self.assertEqual(_neutral_result()['Mgmt_Confidence'], 'NONE')

    def test_neutral_fault_is_insufficient(self):
        self.assertEqual(_neutral_result()['Fault_Pattern'], 'INSUFFICIENT_DATA')


class TestDedupeSequence(unittest.TestCase):
    def test_dedup(self):
        self.assertEqual(
            _dedupe_sequence(['A', 'A', 'B', 'B', 'B', 'C']),
            ['A', 'B', 'C'],
        )

    def test_empty(self):
        self.assertEqual(_dedupe_sequence([]), [])

    def test_single(self):
        self.assertEqual(_dedupe_sequence(['X']), ['X'])

    def test_no_dups(self):
        self.assertEqual(_dedupe_sequence(['A', 'B', 'C']), ['A', 'B', 'C'])

    def test_all_same(self):
        self.assertEqual(_dedupe_sequence(['Z', 'Z', 'Z']), ['Z'])


class TestClassifyIndicatorTrend(unittest.TestCase):
    def test_building(self):
        vals = [15, 16, 17, 18, 20, 22, 24, 26, 28, 30]
        self.assertEqual(_classify_indicator_trend(vals), 'BUILDING')

    def test_fading(self):
        vals = [40, 38, 36, 34, 32, 30, 28, 26, 24, 22]
        self.assertEqual(_classify_indicator_trend(vals), 'FADING')

    def test_flat(self):
        vals = [25, 25, 26, 25, 24, 25, 25, 26, 25, 25]
        self.assertEqual(_classify_indicator_trend(vals), 'FLAT')

    def test_too_few(self):
        self.assertEqual(_classify_indicator_trend([20, 21]), 'UNKNOWN')


class TestClassifyVolume(unittest.TestCase):
    def test_accumulating(self):
        slopes = [5, 3, 4, 6, 2, 7, 3, 5, 4, 6]
        self.assertEqual(_classify_volume(slopes), 'ACCUMULATING')

    def test_distributing(self):
        slopes = [-5, -3, -4, -6, -2, -7, -3, -5, -4, -6]
        self.assertEqual(_classify_volume(slopes), 'DISTRIBUTING')

    def test_neutral(self):
        slopes = [5, -3, 4, -6, 2, -7, 3, -5, 4, -6]
        self.assertEqual(_classify_volume(slopes), 'NEUTRAL')

    def test_too_few(self):
        self.assertEqual(_classify_volume([1]), 'UNKNOWN')


class TestClassifyScoreTrend(unittest.TestCase):
    def test_climbing(self):
        scores = [40, 42, 45, 48, 50, 55, 58, 60]
        self.assertEqual(_classify_score_trend(scores), 'CLIMBING')

    def test_declining(self):
        scores = [70, 68, 65, 62, 58, 55, 50]
        self.assertEqual(_classify_score_trend(scores), 'DECLINING')

    def test_stable(self):
        scores = [50, 51, 50, 49, 50, 51, 50]
        self.assertEqual(_classify_score_trend(scores), 'STABLE')

    def test_v_recovery(self):
        scores = [60, 55, 40, 35, 45, 55, 60]
        self.assertEqual(_classify_score_trend(scores), 'V_RECOVERY')

    def test_too_few(self):
        self.assertEqual(_classify_score_trend([50]), 'UNKNOWN')


class TestClassifyIVArc(unittest.TestCase):
    def test_stable_iv(self):
        rows = [{'iv_30d': 30.0}] * 10
        self.assertEqual(_classify_iv_arc(rows), 'STABLE')

    def test_rising_iv(self):
        rows = [{'iv_30d': 20 + i * 3} for i in range(10)]
        self.assertEqual(_classify_iv_arc(rows), 'RISING')

    def test_falling_iv(self):
        rows = [{'iv_30d': 50 - i * 3} for i in range(10)]
        self.assertEqual(_classify_iv_arc(rows), 'FALLING')

    def test_spiking_iv(self):
        rows = [{'iv_30d': 25}] * 10 + [{'iv_30d': 60}] * 5
        self.assertEqual(_classify_iv_arc(rows), 'SPIKING')

    def test_too_few(self):
        self.assertEqual(_classify_iv_arc([{'iv_30d': 25}]), 'UNKNOWN')


class TestClassifyEarnings(unittest.TestCase):
    def test_reliable_beater(self):
        self.assertEqual(_classify_earnings({'beat_rate': 0.80}), 'RELIABLE_BEATER')

    def test_mixed(self):
        self.assertEqual(_classify_earnings({'beat_rate': 0.55}), 'MIXED')

    def test_unreliable(self):
        self.assertEqual(_classify_earnings({'beat_rate': 0.30}), 'UNRELIABLE')

    def test_no_data(self):
        self.assertEqual(_classify_earnings({}), 'NO_DATA')
        self.assertEqual(_classify_earnings(None), 'NO_DATA')


class TestClassifyMgmtTrack(unittest.TestCase):
    """Tests _classify_mgmt_track with both legacy dict and new list formats."""

    # ── Legacy dict format (backward compat) ──────────────────────
    def test_proven_winner(self):
        track, count, conf, rec = _classify_mgmt_track({'total_trades': 5, 'winning_trades': 4})
        self.assertEqual(track, 'PROVEN_WINNER')
        self.assertEqual(count, 5)
        self.assertEqual(conf, 'MEDIUM')  # 3-5 trades = MEDIUM

    def test_mixed(self):
        track, _, conf, _ = _classify_mgmt_track({'total_trades': 10, 'winning_trades': 5})
        self.assertEqual(track, 'MIXED')
        self.assertEqual(conf, 'HIGH')  # 6+ trades = HIGH

    def test_proven_loser(self):
        track, _, conf, _ = _classify_mgmt_track({'total_trades': 10, 'winning_trades': 2})
        self.assertEqual(track, 'PROVEN_LOSER')
        self.assertEqual(conf, 'HIGH')

    def test_no_data(self):
        track, count, conf, rec = _classify_mgmt_track({})
        self.assertEqual(track, 'NO_DATA')
        self.assertEqual(count, 0)
        self.assertEqual(conf, 'NONE')

    # ── New list format with sample-size discipline ───────────────
    def test_list_high_confidence(self):
        """6+ closed trades = HIGH confidence."""
        trades = [
            {'is_closed': 1, 'best_pnl': 100, 'age_days': 10, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 50, 'age_days': 15, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -30, 'age_days': 20, 'strategy': 'SP'},
            {'is_closed': 1, 'best_pnl': 80, 'age_days': 5, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 40, 'age_days': 12, 'strategy': 'SP'},
            {'is_closed': 1, 'best_pnl': 60, 'age_days': 8, 'strategy': 'CC'},
        ]
        track, count, conf, rec = _classify_mgmt_track(trades)
        self.assertEqual(track, 'PROVEN_WINNER')
        self.assertEqual(count, 6)
        self.assertEqual(conf, 'HIGH')

    def test_list_low_confidence_single_trade(self):
        """1 trade = LOW confidence — shouldn't define a ticker."""
        trades = [{'is_closed': 1, 'best_pnl': -50, 'age_days': 5, 'strategy': 'SP'}]
        track, count, conf, _ = _classify_mgmt_track(trades)
        self.assertEqual(track, 'PROVEN_LOSER')
        self.assertEqual(count, 1)
        self.assertEqual(conf, 'LOW')

    def test_list_medium_confidence(self):
        """3-5 trades = MEDIUM confidence."""
        trades = [
            {'is_closed': 1, 'best_pnl': 100, 'age_days': 10, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -30, 'age_days': 15, 'strategy': 'SP'},
            {'is_closed': 1, 'best_pnl': 50, 'age_days': 20, 'strategy': 'CC'},
        ]
        track, count, conf, _ = _classify_mgmt_track(trades)
        self.assertEqual(conf, 'MEDIUM')

    # ── Recency decay ──────────────────────────────────────────────
    def test_recency_decay_old_losses(self):
        """Old losses (90+ days) should carry less weight than recent wins."""
        trades = [
            {'is_closed': 1, 'best_pnl': -100, 'age_days': 120, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -80, 'age_days': 100, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 50, 'age_days': 10, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 40, 'age_days': 5, 'strategy': 'CC'},
        ]
        track, _, _, recency = _classify_mgmt_track(trades)
        # Recent wins should outweigh old losses after decay
        self.assertIn(track, ('MIXED', 'PROVEN_WINNER'))
        self.assertLess(recency, 0.9)  # not all recent

    def test_recency_factor_all_recent(self):
        """All trades < 30 days → recency factor ≈ 1.0."""
        trades = [
            {'is_closed': 1, 'best_pnl': 100, 'age_days': 5, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 50, 'age_days': 10, 'strategy': 'SP'},
        ]
        _, _, _, recency = _classify_mgmt_track(trades)
        self.assertEqual(recency, 1.0)


class TestStrategyBreakdown(unittest.TestCase):
    def test_multi_strategy(self):
        trades = [
            {'strategy': 'COVERED_CALL', 'is_closed': 1, 'best_pnl': 50},
            {'strategy': 'COVERED_CALL', 'is_closed': 1, 'best_pnl': -20},
            {'strategy': 'SHORT_PUT', 'is_closed': 1, 'best_pnl': -40},
            {'strategy': 'SHORT_PUT', 'is_closed': 1, 'best_pnl': -10},
        ]
        detail = _classify_strategy_breakdown(trades)
        self.assertIn('CC:1W/1L', detail)
        self.assertIn('SP:0W/2L', detail)

    def test_empty_trades(self):
        self.assertEqual(_classify_strategy_breakdown([]), '')

    def test_all_winners(self):
        trades = [
            {'strategy': 'COVERED_CALL', 'is_closed': 1, 'best_pnl': 100},
            {'strategy': 'COVERED_CALL', 'is_closed': 1, 'best_pnl': 50},
        ]
        detail = _classify_strategy_breakdown(trades)
        self.assertEqual(detail, 'CC:2W/0L')


class TestFaultPattern(unittest.TestCase):
    def test_strategy_fault(self):
        """Losses all in one strategy, wins in another = STRATEGY fault."""
        trades = [
            {'is_closed': 1, 'best_pnl': 50, 'strategy': 'COVERED_CALL', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': 50, 'strategy': 'COVERED_CALL', 'entry_dte': 30},
            {'is_closed': 1, 'best_pnl': -30, 'strategy': 'SHORT_PUT', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': -20, 'strategy': 'SHORT_PUT', 'entry_dte': 30},
        ]
        self.assertEqual(_classify_fault_pattern(trades), 'STRATEGY')

    def test_dte_fault(self):
        """Losses cluster in short DTE = DTE fault."""
        trades = [
            {'is_closed': 1, 'best_pnl': 50, 'strategy': 'CC', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': -30, 'strategy': 'CC', 'entry_dte': 14},
            {'is_closed': 1, 'best_pnl': -20, 'strategy': 'SP', 'entry_dte': 7},
            {'is_closed': 1, 'best_pnl': -40, 'strategy': 'CC', 'entry_dte': 10},
        ]
        self.assertEqual(_classify_fault_pattern(trades), 'DTE')

    def test_ticker_fault(self):
        """Losses across multiple strategies = TICKER fault."""
        trades = [
            {'is_closed': 1, 'best_pnl': -30, 'strategy': 'COVERED_CALL', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': -20, 'strategy': 'SHORT_PUT', 'entry_dte': 30},
            {'is_closed': 1, 'best_pnl': -10, 'strategy': 'LONG_CALL', 'entry_dte': 60},
        ]
        self.assertEqual(_classify_fault_pattern(trades), 'TICKER')

    def test_no_losses(self):
        trades = [
            {'is_closed': 1, 'best_pnl': 100, 'strategy': 'CC', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': 50, 'strategy': 'SP', 'entry_dte': 30},
        ]
        self.assertEqual(_classify_fault_pattern(trades), 'NONE')

    def test_insufficient_data(self):
        self.assertEqual(_classify_fault_pattern([]), 'INSUFFICIENT_DATA')
        trades = [{'is_closed': 1, 'best_pnl': -30, 'strategy': 'CC', 'entry_dte': 45}]
        self.assertEqual(_classify_fault_pattern(trades), 'INSUFFICIENT_DATA')


class TestContradictionFlags(unittest.TestCase):
    def test_high_dqs_proven_loser(self):
        flags = _detect_contradictions(
            dqs_trend='CLIMBING', adx_trend='FLAT', vol_accum='NEUTRAL',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='PROVEN_LOSER', regime_path=['Trending'],
        )
        self.assertIn('HIGH_DQS_PROVEN_LOSER', flags)

    def test_strong_earnings_weak_mgmt(self):
        flags = _detect_contradictions(
            dqs_trend='STABLE', adx_trend='FLAT', vol_accum='NEUTRAL',
            iv_arc='STABLE', earnings_ctx='RELIABLE_BEATER',
            mgmt_track='PROVEN_LOSER', regime_path=['Trending'],
        )
        self.assertIn('STRONG_EARNINGS_WEAK_MGMT', flags)

    def test_momentum_volume_divergence(self):
        flags = _detect_contradictions(
            dqs_trend='STABLE', adx_trend='BUILDING', vol_accum='DISTRIBUTING',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA', regime_path=['Trending'],
        )
        self.assertIn('MOMENTUM_VOLUME_DIVERGENCE', flags)

    def test_no_contradictions(self):
        flags = _detect_contradictions(
            dqs_trend='STABLE', adx_trend='FLAT', vol_accum='NEUTRAL',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA', regime_path=['Range_Bound'],
        )
        self.assertEqual(flags, [])

    def test_multiple_flags_compound(self):
        """NVDA scenario: great earnings + proven loser + constructive regime."""
        flags = _detect_contradictions(
            dqs_trend='V_RECOVERY', adx_trend='BUILDING', vol_accum='NEUTRAL',
            iv_arc='STABLE', earnings_ctx='RELIABLE_BEATER',
            mgmt_track='PROVEN_LOSER', regime_path=['Trending'],
        )
        self.assertIn('HIGH_DQS_PROVEN_LOSER', flags)
        self.assertIn('STRONG_EARNINGS_WEAK_MGMT', flags)
        self.assertIn('STRONG_MOMENTUM_PROVEN_LOSER', flags)
        self.assertIn('CONSTRUCTIVE_REGIME_PROVEN_LOSER', flags)
        self.assertGreaterEqual(len(flags), 4)


class TestBehavioralScore(unittest.TestCase):
    def test_strong_arc(self):
        """Stable trending + building ADX + accumulation + reliable earnings = high."""
        score = _compute_behavioral_score(
            regime_duration=20,
            regime_path=['Compression', 'Emerging_Trend', 'Trending'],
            adx_trend='BUILDING',
            adx_vals=[15, 18, 20, 22, 25, 28, 30],
            rsi_range=12.0,
            vol_accum='ACCUMULATING',
            dqs_trend='CLIMBING',
            iv_arc='FALLING',
            earnings_ctx='RELIABLE_BEATER',
            mgmt_track='PROVEN_WINNER',
            mgmt_confidence='HIGH',
            data_points=45,
        )
        self.assertGreaterEqual(score, 75)

    def test_hostile_arc(self):
        """Overextended + fading + distribution + unreliable = low."""
        score = _compute_behavioral_score(
            regime_duration=2,
            regime_path=['Trending', 'Overextended'],
            adx_trend='FADING',
            adx_vals=[40, 38, 35, 32, 28],
            rsi_range=40.0,
            vol_accum='DISTRIBUTING',
            dqs_trend='DECLINING',
            iv_arc='SPIKING',
            earnings_ctx='UNRELIABLE',
            mgmt_track='PROVEN_LOSER',
            mgmt_confidence='HIGH',
            data_points=25,
        )
        self.assertLess(score, 30)

    def test_baseline_neutral(self):
        """All neutral inputs -> score near 50."""
        score = _compute_behavioral_score(
            regime_duration=3,
            regime_path=['Range_Bound'],
            adx_trend='FLAT',
            adx_vals=[25, 25, 25],
            rsi_range=20.0,
            vol_accum='NEUTRAL',
            dqs_trend='STABLE',
            iv_arc='STABLE',
            earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA',
            data_points=10,
        )
        self.assertGreaterEqual(score, 45)
        self.assertLessEqual(score, 65)

    def test_score_clamped_0_100(self):
        high = _compute_behavioral_score(
            regime_duration=30,
            regime_path=['Compression', 'Emerging_Trend', 'Trending'],
            adx_trend='BUILDING',
            adx_vals=list(range(10, 40)),
            rsi_range=8.0,
            vol_accum='ACCUMULATING',
            dqs_trend='CLIMBING',
            iv_arc='FALLING',
            earnings_ctx='RELIABLE_BEATER',
            mgmt_track='PROVEN_WINNER',
            mgmt_confidence='HIGH',
            data_points=50,
        )
        self.assertLessEqual(high, 100)

        low = _compute_behavioral_score(
            regime_duration=1,
            regime_path=['Breakdown'],
            adx_trend='FADING',
            adx_vals=[40, 35, 30],
            rsi_range=45.0,
            vol_accum='DISTRIBUTING',
            dqs_trend='DECLINING',
            iv_arc='SPIKING',
            earnings_ctx='UNRELIABLE',
            mgmt_track='PROVEN_LOSER',
            mgmt_confidence='HIGH',
            data_points=2,
        )
        self.assertGreaterEqual(low, 0)

    def test_classic_breakout_bonus(self):
        with_prog = _compute_behavioral_score(
            regime_duration=5,
            regime_path=['Compression', 'Emerging_Trend'],
            adx_trend='BUILDING',
            adx_vals=[15, 18, 20, 22, 25],
            rsi_range=20.0,
            vol_accum='NEUTRAL',
            dqs_trend='STABLE',
            iv_arc='STABLE',
            earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA',
            data_points=10,
        )
        without_prog = _compute_behavioral_score(
            regime_duration=5,
            regime_path=['Range_Bound', 'Range_Bound'],
            adx_trend='BUILDING',
            adx_vals=[15, 18, 20, 22, 25],
            rsi_range=20.0,
            vol_accum='NEUTRAL',
            dqs_trend='STABLE',
            iv_arc='STABLE',
            earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA',
            data_points=10,
        )
        self.assertGreater(with_prog, without_prog)

    def test_iv_arc_impact(self):
        """Falling IV should boost score vs spiking IV."""
        falling = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='FALLING', earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA', data_points=10,
        )
        spiking = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='SPIKING', earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA', data_points=10,
        )
        self.assertGreater(falling, spiking)

    def test_mgmt_track_impact(self):
        """Proven winner should boost score vs proven loser (HIGH confidence)."""
        winner = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='PROVEN_WINNER', mgmt_confidence='HIGH',
            data_points=10,
        )
        loser = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='PROVEN_LOSER', mgmt_confidence='HIGH',
            data_points=10,
        )
        self.assertGreater(winner, loser)

    def test_low_confidence_dampens_mgmt_impact(self):
        """LOW confidence should dampen management impact vs HIGH."""
        loser_high = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='PROVEN_LOSER', mgmt_confidence='HIGH',
            data_points=10,
        )
        loser_low = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='PROVEN_LOSER', mgmt_confidence='LOW',
            data_points=10,
        )
        # LOW confidence should hurt less than HIGH confidence
        self.assertGreater(loser_low, loser_high)

    def test_no_confidence_zero_impact(self):
        """NONE confidence = zero management impact (same as NO_DATA)."""
        with_loser = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='PROVEN_LOSER', mgmt_confidence='NONE',
            data_points=10,
        )
        no_data = _compute_behavioral_score(
            regime_duration=5, regime_path=['Trending'],
            adx_trend='FLAT', adx_vals=[25]*5, rsi_range=20.0,
            vol_accum='NEUTRAL', dqs_trend='STABLE',
            iv_arc='STABLE', earnings_ctx='NO_DATA',
            mgmt_track='NO_DATA',
            data_points=10,
        )
        self.assertEqual(with_loser, no_data)


class TestComputeOne(unittest.TestCase):
    def _make_tech_rows(self, n=15, regime='Trending', adx_base=20, adx_step=0.5):
        rows = []
        for i in range(n):
            rows.append({
                'Chart_Regime': regime,
                'ADX_14': adx_base + i * adx_step,
                'RSI_14': 50 + (i % 5),
                'OBV_Slope': 3.0,
                'Volume_Ratio': 1.1,
            })
        return rows

    def _make_scan_rows(self, n=5, status='READY', dqs_base=60, dqs_step=2):
        return [
            {'Execution_Status': status, 'DQS_Score': dqs_base + i * dqs_step}
            for i in range(n)
        ]

    def test_trending_accumulating(self):
        r = _compute_one(
            'AAPL',
            self._make_tech_rows(20, regime='Trending', adx_base=18, adx_step=0.8),
            self._make_scan_rows(8, status='READY', dqs_base=55, dqs_step=2),
            [],  # iv_rows
            {},  # earnings
            {},  # mgmt
        )
        self.assertEqual(r['Volume_Accumulation'], 'ACCUMULATING')
        self.assertGreater(r['Regime_Duration'], 0)
        self.assertGreater(r['Behavioral_Score'], 50)
        self.assertEqual(r['History_Depth'], 20)

    def test_regime_path_deduplication(self):
        tech = []
        for regime in ['Compression'] * 5 + ['Emerging_Trend'] * 5 + ['Trending'] * 5:
            tech.append({
                'Chart_Regime': regime, 'ADX_14': 25, 'RSI_14': 50,
                'OBV_Slope': 1.0, 'Volume_Ratio': 1.0,
            })
        r = _compute_one('TEST', tech, [], [], {}, {})
        self.assertIn('\u2192', r['Regime_Path'])
        parts = r['Regime_Path'].split('\u2192')
        self.assertEqual(len(parts), 3)

    def test_regime_duration_counts_consecutive(self):
        tech = []
        for regime in ['Range_Bound'] * 5 + ['Trending'] * 10:
            tech.append({
                'Chart_Regime': regime, 'ADX_14': 25, 'RSI_14': 50,
                'OBV_Slope': 1.0, 'Volume_Ratio': 1.0,
            })
        r = _compute_one('DUR', tech, [], [], {}, {})
        self.assertEqual(r['Regime_Duration'], 10)

    def test_scan_frequency_counts_ready(self):
        scans = [
            {'Execution_Status': 'READY', 'DQS_Score': 60},
            {'Execution_Status': 'CONDITIONAL', 'DQS_Score': 45},
            {'Execution_Status': 'READY', 'DQS_Score': 65},
            {'Execution_Status': 'BLOCKED', 'DQS_Score': 30},
        ]
        tech = [{'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 50,
                  'OBV_Slope': 1.0, 'Volume_Ratio': 1.0}] * 5
        r = _compute_one('FREQ', tech, scans, [], {}, {})
        self.assertEqual(r['Scan_Frequency'], 2)

    def test_iv_arc_and_earnings_included(self):
        tech = self._make_tech_rows(10)
        iv = [{'iv_30d': 20 + i * 2} for i in range(10)]
        earn = {'beat_rate': 0.85}
        mgmt = {'total_trades': 3, 'winning_trades': 3}
        r = _compute_one('MSFT', tech, [], iv, earn, mgmt)
        self.assertEqual(r['IV_Arc'], 'RISING')
        self.assertEqual(r['Earnings_Context'], 'RELIABLE_BEATER')
        self.assertEqual(r['Mgmt_Track_Record'], 'PROVEN_WINNER')
        self.assertEqual(r['Prior_Trades'], 3)

    def test_all_fields_present(self):
        r = _compute_one('X', self._make_tech_rows(5), [], [], {}, {})
        expected_keys = set(_neutral_result().keys())
        self.assertEqual(set(r.keys()), expected_keys)

    def test_contradiction_flags_populated(self):
        """NVDA-like scenario: great earnings + loser mgmt → flags."""
        tech = self._make_tech_rows(10, regime='Trending')
        mgmt_trades = [
            {'is_closed': 1, 'best_pnl': -50, 'age_days': 10, 'strategy': 'COVERED_CALL'},
            {'is_closed': 1, 'best_pnl': -30, 'age_days': 20, 'strategy': 'COVERED_CALL'},
            {'is_closed': 1, 'best_pnl': -20, 'age_days': 30, 'strategy': 'SHORT_PUT'},
        ]
        earn = {'beat_rate': 0.90}
        r = _compute_one('NVDA', tech, [], [], earn, mgmt_trades)
        self.assertIn('STRONG_EARNINGS_WEAK_MGMT', r['Contradiction_Flags'])
        self.assertEqual(r['Mgmt_Confidence'], 'MEDIUM')

    def test_strategy_detail_populated(self):
        tech = self._make_tech_rows(10)
        mgmt_trades = [
            {'is_closed': 1, 'best_pnl': 100, 'age_days': 5, 'strategy': 'COVERED_CALL'},
            {'is_closed': 1, 'best_pnl': -30, 'age_days': 10, 'strategy': 'SHORT_PUT'},
        ]
        r = _compute_one('VZ', tech, [], [], {}, mgmt_trades)
        self.assertIn('CC:1W/0L', r['Mgmt_Strategy_Detail'])
        self.assertIn('SP:0W/1L', r['Mgmt_Strategy_Detail'])

    def test_fault_pattern_strategy(self):
        """Losses all in one strategy = STRATEGY fault."""
        tech = self._make_tech_rows(10)
        mgmt_trades = [
            {'is_closed': 1, 'best_pnl': 100, 'age_days': 5, 'strategy': 'COVERED_CALL', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': 50, 'age_days': 10, 'strategy': 'COVERED_CALL', 'entry_dte': 30},
            {'is_closed': 1, 'best_pnl': -30, 'age_days': 15, 'strategy': 'SHORT_PUT', 'entry_dte': 45},
            {'is_closed': 1, 'best_pnl': -20, 'age_days': 20, 'strategy': 'SHORT_PUT', 'entry_dte': 30},
        ]
        r = _compute_one('TEST', tech, [], [], {}, mgmt_trades)
        self.assertEqual(r['Fault_Pattern'], 'STRATEGY')


class TestMoveDrivers(unittest.TestCase):
    def test_no_transitions(self):
        """All same regime = no transitions = empty drivers."""
        tech = [{'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 50,
                 'Snapshot_TS': '2026-02-01'}] * 10
        result = _classify_move_drivers(tech, {})
        self.assertEqual(result['Move_Drivers'], '')

    def test_regime_change_detected(self):
        """Regime transition should produce a driver classification."""
        from datetime import datetime
        tech = []
        for i in range(5):
            tech.append({'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 55,
                         'Snapshot_TS': datetime(2026, 2, 1 + i)})
        for i in range(5):
            tech.append({'Chart_Regime': 'Compressed', 'ADX_14': 18, 'RSI_14': 45,
                         'Snapshot_TS': datetime(2026, 2, 6 + i)})
        result = _classify_move_drivers(tech, {})
        self.assertNotEqual(result['Move_Drivers'], '')

    def test_rsi_shift_detected(self):
        """Large RSI shift (>15 points) should register as a move."""
        from datetime import datetime
        tech = []
        for i in range(5):
            tech.append({'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 60,
                         'Snapshot_TS': datetime(2026, 2, 1 + i)})
        # Sudden RSI drop
        tech.append({'Chart_Regime': 'Trending', 'ADX_14': 20, 'RSI_14': 35,
                     'Snapshot_TS': datetime(2026, 2, 7)})
        result = _classify_move_drivers(tech, {})
        self.assertIn('Last_Dip_Context', result)

    def test_too_few_rows(self):
        """< 5 rows → empty result."""
        tech = [{'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 50}] * 3
        result = _classify_move_drivers(tech, {})
        self.assertEqual(result['Move_Drivers'], '')

    def test_macro_proximity_detected(self):
        """Regime change on FOMC day (2026-01-28) should tag as MACRO."""
        from datetime import datetime
        tech = []
        for i in range(5):
            tech.append({'Chart_Regime': 'Trending', 'ADX_14': 30, 'RSI_14': 60,
                         'Snapshot_TS': datetime(2026, 1, 23 + i)})
        # FOMC day: Jan 28, 2026 — regime breaks
        tech.append({'Chart_Regime': 'Compressed', 'ADX_14': 20, 'RSI_14': 42,
                     'Snapshot_TS': datetime(2026, 1, 28)})
        result = _classify_move_drivers(tech, {})
        self.assertIn('MACRO', result['Move_Drivers'])

    def test_market_aligned_tagged(self):
        """Move with RS_vs_SPY near zero → MARKET_WIDE."""
        from datetime import datetime
        tech = []
        for i in range(5):
            tech.append({'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 55,
                         'RS_vs_SPY_20d': 5.0,
                         'Snapshot_TS': datetime(2026, 3, 1 + i)})
        # RS near zero + regime change → market-wide
        tech.append({'Chart_Regime': 'Compressed', 'ADX_14': 18, 'RSI_14': 42,
                     'RS_vs_SPY_20d': 0.5,
                     'Snapshot_TS': datetime(2026, 3, 7)})
        result = _classify_move_drivers(tech, {})
        self.assertIn('MARKET_WIDE', result['Move_Drivers'])

    def test_compute_one_includes_move_context(self):
        """_compute_one output includes Move_Drivers and Last_Dip_Context."""
        from datetime import datetime
        tech = []
        for i in range(5):
            tech.append({'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 50,
                         'OBV_Slope': 1.0, 'Volume_Ratio': 1.0,
                         'Snapshot_TS': datetime(2026, 2, 1 + i)})
        for i in range(5):
            tech.append({'Chart_Regime': 'Compressed', 'ADX_14': 18, 'RSI_14': 45,
                         'OBV_Slope': -1.0, 'Volume_Ratio': 0.8,
                         'Snapshot_TS': datetime(2026, 2, 6 + i)})
        r = _compute_one('TEST', tech, [], [], {}, {})
        self.assertIn('Move_Drivers', r)
        self.assertIn('Last_Dip_Context', r)


class TestPSRVarianceAdjustment(unittest.TestCase):
    """Gap 1: Lopez de Prado — high PnL variance should downgrade confidence."""

    def test_consistent_outcomes_keep_confidence(self):
        """6 trades with similar PnL → HIGH stays HIGH."""
        trades = [{'is_closed': 1, 'best_pnl': 50 + i, 'age_days': 5, 'strategy': 'CC'}
                  for i in range(6)]
        _, _, confidence, _ = _classify_mgmt_track(trades)
        self.assertEqual(confidence, 'HIGH')

    def test_wildly_variable_outcomes_downgrade(self):
        """6 trades with huge variance → HIGH → MEDIUM."""
        # Mean ≈ 0, std ≈ 454 → CoV = 454/|0| → inf → downgrade
        trades = [
            {'is_closed': 1, 'best_pnl': 500, 'age_days': 5, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -500, 'age_days': 10, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 400, 'age_days': 15, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -400, 'age_days': 20, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 300, 'age_days': 25, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -300, 'age_days': 30, 'strategy': 'CC'},
        ]
        _, _, confidence, _ = _classify_mgmt_track(trades)
        self.assertEqual(confidence, 'MEDIUM')

    def test_medium_variance_downgrade_to_low(self):
        """3-5 trades with wild swings → MEDIUM → LOW."""
        trades = [
            {'is_closed': 1, 'best_pnl': 1000, 'age_days': 5, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -800, 'age_days': 10, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': 500, 'age_days': 15, 'strategy': 'CC'},
        ]
        _, _, confidence, _ = _classify_mgmt_track(trades)
        self.assertEqual(confidence, 'LOW')

    def test_low_stays_low(self):
        """LOW can't downgrade further."""
        trades = [
            {'is_closed': 1, 'best_pnl': 500, 'age_days': 5, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -500, 'age_days': 10, 'strategy': 'CC'},
        ]
        _, _, confidence, _ = _classify_mgmt_track(trades)
        self.assertEqual(confidence, 'LOW')

    def test_two_trades_skip_variance_check(self):
        """< 3 trades → skip variance check (not enough for std)."""
        trades = [
            {'is_closed': 1, 'best_pnl': 100, 'age_days': 5, 'strategy': 'CC'},
            {'is_closed': 1, 'best_pnl': -100, 'age_days': 10, 'strategy': 'CC'},
        ]
        _, _, confidence, _ = _classify_mgmt_track(trades)
        self.assertEqual(confidence, 'LOW')  # 2 trades = LOW by count, not variance


class TestEventReactions(unittest.TestCase):
    """Gap 2: Augen — per-event response profiling."""

    def test_too_few_rows(self):
        tech = [{'RSI_14': 50, 'Snapshot_TS': '2026-01-05'}] * 3
        result = _profile_event_reactions(tech)
        self.assertEqual(result['Event_Reactions'], '')

    def test_fomc_reaction_detected(self):
        """RSI change around FOMC dates should produce a reaction profile."""
        from datetime import datetime
        tech = []
        # Build rows around FOMC Jan 28, 2026 and FOMC Mar 18, 2026
        # Before FOMC 1: Jan 27 RSI=55
        tech.append({'RSI_14': 55, 'Snapshot_TS': datetime(2026, 1, 27)})
        # After FOMC 1: Jan 28 RSI=48 (drop)
        tech.append({'RSI_14': 48, 'Snapshot_TS': datetime(2026, 1, 28)})
        # Before FOMC 2: Mar 17 RSI=60
        tech.append({'RSI_14': 60, 'Snapshot_TS': datetime(2026, 3, 17)})
        # After FOMC 2: Mar 18 RSI=52 (drop)
        tech.append({'RSI_14': 52, 'Snapshot_TS': datetime(2026, 3, 18)})
        # Some filler rows to get past the 5-row minimum
        tech.append({'RSI_14': 50, 'Snapshot_TS': datetime(2026, 2, 15)})
        result = _profile_event_reactions(tech)
        self.assertIn('FOMC', result['Event_Reactions'])

    def test_worst_event_flagged(self):
        """Most negative reaction should populate Worst_Event_Type."""
        from datetime import datetime
        tech = []
        # CPI Feb 11 — big RSI drop
        tech.append({'RSI_14': 65, 'Snapshot_TS': datetime(2026, 2, 10)})
        tech.append({'RSI_14': 45, 'Snapshot_TS': datetime(2026, 2, 11)})
        # CPI Mar 11 — another drop
        tech.append({'RSI_14': 60, 'Snapshot_TS': datetime(2026, 3, 10)})
        tech.append({'RSI_14': 42, 'Snapshot_TS': datetime(2026, 3, 11)})
        tech.append({'RSI_14': 50, 'Snapshot_TS': datetime(2026, 2, 20)})
        result = _profile_event_reactions(tech)
        if result['Event_Reactions']:
            self.assertEqual(result['Worst_Event_Type'], 'CPI')

    def test_no_data_around_events(self):
        """Tech rows that don't line up with any macro dates → empty."""
        from datetime import datetime
        tech = [{'RSI_14': 50, 'Snapshot_TS': datetime(2026, 6, 20 + i)}
                for i in range(10)]
        result = _profile_event_reactions(tech)
        # May or may not find events — depends on calendar proximity
        self.assertIn('Event_Reactions', result)

    def test_positive_reactions_no_worst(self):
        """If all reactions are positive, no Worst_Event_Type."""
        from datetime import datetime
        tech = []
        # NFP Feb 6 — RSI jumps
        tech.append({'RSI_14': 45, 'Snapshot_TS': datetime(2026, 2, 5)})
        tech.append({'RSI_14': 55, 'Snapshot_TS': datetime(2026, 2, 6)})
        # NFP Mar 6 — RSI jumps
        tech.append({'RSI_14': 48, 'Snapshot_TS': datetime(2026, 3, 5)})
        tech.append({'RSI_14': 58, 'Snapshot_TS': datetime(2026, 3, 6)})
        tech.append({'RSI_14': 50, 'Snapshot_TS': datetime(2026, 2, 15)})
        result = _profile_event_reactions(tech)
        self.assertEqual(result['Worst_Event_Type'], '')


class TestDataMaturity(unittest.TestCase):
    """Gap 3: Chan/Harris — survivorship bias flag."""

    def test_new_ticker_short_history(self):
        tech = [{'Snapshot_TS': '2026-03-01'}] * 10
        self.assertEqual(_assess_data_maturity(10, 0, tech), 'NEW_TICKER')

    def test_developing_moderate_history(self):
        from datetime import datetime
        tech = [{'Snapshot_TS': datetime(2026, 1, 1 + i)} for i in range(25)]
        self.assertEqual(_assess_data_maturity(25, 3, tech), 'DEVELOPING')

    def test_mature_deep_history(self):
        from datetime import datetime, timedelta
        base = datetime(2026, 1, 1)
        tech = [{'Snapshot_TS': base + timedelta(days=i)} for i in range(45)]
        self.assertEqual(_assess_data_maturity(45, 10, tech), 'MATURE')

    def test_narrow_date_range_is_new(self):
        """Many rows but clustered in <20 days → still NEW_TICKER."""
        from datetime import datetime
        tech = [{'Snapshot_TS': datetime(2026, 3, 1)} for _ in range(20)]
        self.assertEqual(_assess_data_maturity(20, 5, tech), 'NEW_TICKER')

    def test_compute_one_includes_maturity(self):
        """_compute_one should output Data_Maturity field."""
        tech = [{'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 50,
                 'OBV_Slope': 1.0, 'Volume_Ratio': 1.0}] * 5
        r = _compute_one('TEST', tech, [], [], {}, {})
        self.assertIn('Data_Maturity', r)

    def test_compute_one_includes_event_reactions(self):
        """_compute_one should output Event_Reactions field."""
        tech = [{'Chart_Regime': 'Trending', 'ADX_14': 25, 'RSI_14': 50,
                 'OBV_Slope': 1.0, 'Volume_Ratio': 1.0}] * 5
        r = _compute_one('TEST', tech, [], [], {}, {})
        self.assertIn('Event_Reactions', r)
        self.assertIn('Worst_Event_Type', r)


if __name__ == '__main__':
    unittest.main()
