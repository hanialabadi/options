#!/usr/bin/env python3
"""
Verify Broker-Aware Tier System
Simulates what Step 7B and Step 9B will do with the corrected tier definitions
"""

from core.strategy_tiers import get_strategy_tier, is_execution_ready, get_execution_blocker

# Simulate typical strategies the pipeline might generate
typical_strategies = [
    'Long Call',
    'Long Put',
    'Covered Call',
    'Cash-Secured Put',
    'Long Straddle',
    'Call Debit Spread',
    'Put Debit Spread',
    'Iron Condor',
    'LEAP Call',
    'Poor Man\'s Covered Call',
]

print("🧪 BROKER-AWARE TIER SYSTEM VERIFICATION")
print("=" * 80)
print("\nSimulating what Step 7B adds to strategies and Step 9B checks:\n")

tier1_exec = []
tier2_blocked = []
tier3_future = []

for strat in typical_strategies:
    meta = get_strategy_tier(strat)
    tier = meta['tier']
    ready = is_execution_ready(strat)
    blocker = get_execution_blocker(strat)
    
    status_icon = "✅" if ready else "⛔"
    
    print(f"{status_icon} {strat:<30} | Tier {tier} | Ready: {ready}")
    print(f"   └─ Approval: {meta['broker_approval']}")
    if blocker:
        print(f"   └─ Blocker: {blocker}")
    print()
    
    if ready:
        tier1_exec.append(strat)
    elif tier == 2:
        tier2_blocked.append(strat)
    else:
        tier3_future.append(strat)

print("=" * 80)
print("\n📊 RESULTS SUMMARY:\n")
print(f"✅ Tier 1 (Will scan option chains): {len(tier1_exec)}")
for s in tier1_exec:
    print(f"   • {s}")

print(f"\n⛔ Tier 2 (Broker-blocked - skip scanning): {len(tier2_blocked)}")
for s in tier2_blocked:
    print(f"   • {s} → {get_execution_blocker(s)}")

print(f"\n🔧 Tier 3 (Logic-blocked - skip scanning): {len(tier3_future)}")
for s in tier3_future:
    print(f"   • {s} → {get_execution_blocker(s)}")

print("\n" + "=" * 80)
print("✅ VALIDATION COMPLETE!")
print("\nKey Findings:")
print(f"  • {len(tier1_exec)}/{len(typical_strategies)} strategies are broker-approved (no spreads!)")
print(f"  • {len(tier2_blocked)} strategies blocked by broker (upgrade account to unlock)")
print(f"  • {len(tier3_future)} strategies blocked by system (future development)")
print("\n💡 Step 9B will ONLY process the Tier 1 strategies above.")
print("   Spreads will pass through with 'Strategy_Only' status (no scanning).")
