#!/usr/bin/env python3
"""
Apply execution equivalence fixes to dashboard.py
This script safely applies all 5 fixes to force CLI-style execution
"""
import re

# Read the file
with open('streamlit_app/dashboard.py', 'r') as f:
    content = f.read()

print("📝 Applying 5 execution equivalence fixes...")

# Fix 1: Remove Live Mode checkbox (lines ~406-418)
print("Fix 1: Removing Live Mode checkbox...")
pattern1 = r'''    # Step 0 Integration Toggle \(PROMINENT\)
    st\.divider\(\)
    col_toggle1, col_toggle2 = st\.columns\(\[1, 4\]\)
    with col_toggle1:
        use_live_snapshot = st\.checkbox\(
            "🔴 \*\*LIVE MODE\*\*",
            value=False,
            help="Use Live Schwab Snapshot from Step 0"
        \)
    with col_toggle2:
        if use_live_snapshot:
            st\.success\("✅ \*\*STEP 0 ACTIVE\*\* - Will load latest Schwab snapshot \(bypasses scraper & full pipeline\)"\)
        else:
            st\.info\("ℹ️ Legacy mode - Uses data source from sidebar \+ runs full pipeline"\)
    st\.divider\(\)
    
    # Configuration for the full pipeline run'''

replacement1 = '''    # Configuration for the full pipeline run'''

content = re.sub(pattern1, replacement1, content, flags=re.MULTILINE)

# Fix 2 & 4: Remove Live Mode execution branch and unify button label  
print("Fix 2 & 4: Removing Live Mode execution and unifying button label...")
pattern2 = r'''        button_label = "▶️ Load Step 2 Data" if use_live_snapshot else "▶️ Run Full Pipeline"
        if st\.button\(button_label, type="primary", use_container_width=True\):'''

replacement2 = '''        if st.button("▶️ Run Full Pipeline", type="primary", use_container_width=True):'''

content = re.sub(pattern2, replacement2, content)

# Remove the Live Mode execution block
pattern2b = r'''                # BRIDGE MODE: Load Step 2 directly when live snapshot enabled
                if use_live_snapshot:.*?else:
                    # LEGACY MODE: Run full pipeline
                    st\.session_state\['live_snapshot_mode'\] = False'''

replacement2b = '''                # Run full pipeline (ALWAYS - no Live Mode fallback)
                st.session_state['live_snapshot_mode'] = False'''

content = re.sub(pattern2b, replacement2b, content, flags=re.DOTALL)

print(f"✅ Fixes applied successfully")
print(f"📊 File length: {len(content)} characters")

# Write back
with open('streamlit_app/dashboard.py', 'w') as f:
    f.write(content)

print("✅ Dashboard updated! Run tests to verify.")
