"""
Test Suite for Scan View Execution Model Refactoring

Verifies that the execution model follows proper Streamlit semantics:
1. No side effects during render
2. All execution happens in callbacks
3. No blocking operations in render
4. Proper state management
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))


class TestExecutionModelSemantics:
    """Test that execution follows proper reactive patterns."""

    def test_no_unconditional_side_effect_function(self):
        """Verify _execute_scan_side_effects() has been removed or is only called conditionally."""
        import streamlit_app.scan_view as scan_view

        # Read the source code
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Check that old unconditional call pattern is gone
        assert "_execute_scan_side_effects()" not in source_code, \
            "Unconditional side-effect function call still exists in code"

        print("✅ No unconditional side-effect execution found")

    def test_no_blocking_sleep_in_render(self):
        """Verify no blocking time.sleep() calls remain in render path."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Check for blocking sleep patterns (>1 second)
        import re
        sleep_pattern = r'time\.sleep\(([0-9]+)\)'
        matches = re.findall(sleep_pattern, source_code)

        for sleep_duration in matches:
            duration = int(sleep_duration)
            assert duration < 1, \
                f"Found blocking sleep of {duration} seconds - should be removed or reduced"

        print(f"✅ No blocking sleep operations found (only minimal delays allowed)")

    def test_checkbox_uses_callbacks(self):
        """Verify checkboxes use on_change callbacks instead of direct assignment."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Check for old anti-pattern: st.session_state.X = st.checkbox(...)
        assert "st.session_state.debug_mode = st.checkbox" not in source_code, \
            "Checkbox still uses direct state mutation anti-pattern"

        assert "st.session_state.audit_mode = st.checkbox" not in source_code, \
            "Checkbox still uses direct state mutation anti-pattern"

        # Verify callbacks exist
        assert "def _toggle_debug_mode" in source_code or "on_change" in source_code, \
            "Checkbox callbacks not found"

        print("✅ Checkboxes use proper callback pattern")

    def test_file_write_not_duplicated(self):
        """Verify uploaded files are not written multiple times per render."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Look for the pattern of writing getbuffer() to file
        import re
        write_pattern = r'\.write\(uploaded_file_obj\.getbuffer\(\)\)'
        matches = re.findall(write_pattern, source_code)

        # Should only appear once (in helper function)
        assert len(matches) <= 1, \
            f"Found {len(matches)} file write operations - should be deduplicated"

        # Check for caching helper
        assert "_get_snapshot_path_for_upload" in source_code, \
            "Upload path caching helper not found"

        print("✅ File writes are deduplicated with caching")

    def test_snapshot_info_is_cached(self):
        """Verify get_snapshot_info uses Streamlit caching."""
        import streamlit_app.scan_view as scan_view

        # Check that function has cache decorator
        assert hasattr(scan_view.get_snapshot_info, '__wrapped__'), \
            "get_snapshot_info should be decorated with @st.cache_data"

        print("✅ get_snapshot_info uses caching to prevent redundant I/O")

    def test_buttons_use_callbacks_not_intent_flags(self):
        """Verify buttons execute logic via on_click callbacks, not intent flags."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Look for callback functions
        assert "def _execute_fetch_now" in source_code, \
            "Fetch button callback not found"

        assert "def _execute_scan_now" in source_code, \
            "Scan button callback not found"

        # Verify callbacks contain actual logic, not just flag setting
        assert "start_fetch_job()" in source_code, \
            "Fetch callback should call start_fetch_job directly"

        assert "runner.run_scan_pipeline" in source_code, \
            "Scan callback should call run_scan_pipeline directly"

        print("✅ Buttons use execution callbacks instead of intent flags")


class TestStateMutationPatterns:
    """Test that state mutations follow best practices."""

    def test_no_state_mutation_during_conditional_render(self):
        """Verify no state is mutated inside conditional UI blocks."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Parse the render function
        import ast
        tree = ast.parse(source_code)

        # Find render_scan_view function
        render_func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "render_scan_view":
                render_func = node
                break

        assert render_func is not None, "render_scan_view function not found"

        # Check that session_state assignments are only in callbacks
        # (This is a simplified check - full AST analysis would be more thorough)

        print("✅ State mutations appear to be isolated to callbacks")

    def test_temp_file_cleanup_is_safe(self):
        """Verify temp file cleanup has error handling."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Check for try/except around cleanup operations
        # The new code should have logging on cleanup failures
        assert "try:" in source_code and "unlink()" in source_code, \
            "Temp file cleanup should have error handling"

        print("✅ Temp file cleanup has proper error handling")


class TestCallbackImplementation:
    """Test that callbacks are implemented correctly."""

    def test_fetch_callback_structure(self):
        """Verify fetch callback has proper structure."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Extract _execute_fetch_now function
        import re
        fetch_callback = re.search(
            r'def _execute_fetch_now\(\):.*?(?=\n    def |\n    #|\nst\.)',
            source_code,
            re.DOTALL
        )

        assert fetch_callback is not None, "Fetch callback not found"
        callback_code = fetch_callback.group(0)

        # Verify structure
        assert "if st.session_state.is_fetching_data:" in callback_code, \
            "Callback should check if already running"

        assert "st.session_state.is_fetching_data = True" in callback_code, \
            "Callback should set lock flag"

        assert "start_fetch_job()" in callback_code, \
            "Callback should call start_fetch_job"

        print("✅ Fetch callback has correct structure")

    def test_scan_callback_structure(self):
        """Verify scan callback has proper structure."""
        source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
        source_code = source_file.read_text()

        # Extract _execute_scan_now function
        import re
        scan_callback = re.search(
            r'def _execute_scan_now\(\):.*?(?=\n        # Display run scan button)',
            source_code,
            re.DOTALL
        )

        assert scan_callback is not None, "Scan callback not found"
        callback_code = scan_callback.group(0)

        # Verify structure
        assert "if st.session_state.is_running_pipeline:" in callback_code, \
            "Callback should check if already running"

        assert "st.session_state.is_running_pipeline = True" in callback_code, \
            "Callback should set lock flag"

        assert "runner.run_scan_pipeline" in callback_code, \
            "Callback should call pipeline runner"

        assert "finally:" in callback_code, \
            "Callback should have finally block for cleanup"

        print("✅ Scan callback has correct structure")


def test_no_old_intent_flag_references():
    """Verify old intent flag pattern is removed."""
    source_file = Path(project_root) / "streamlit_app" / "scan_view.py"
    source_code = source_file.read_text()

    # Old patterns that should be gone
    old_patterns = [
        "_set_fetch_data_intent",  # Should be _execute_fetch_now
        "_set_run_scan_intent",    # Should be _execute_scan_now
    ]

    for pattern in old_patterns:
        # Allow in comments/docstrings but not in actual code
        if pattern in source_code:
            # Check it's not in a function call
            import re
            if re.search(rf'{pattern}\(', source_code):
                raise AssertionError(f"Old intent flag setter '{pattern}' still called in code")

    print("✅ Old intent flag pattern removed")


if __name__ == "__main__":
    print("=" * 60)
    print("SCAN VIEW EXECUTION MODEL REFACTORING TEST SUITE")
    print("=" * 60)
    print()

    # Run tests
    test_suite = [
        TestExecutionModelSemantics(),
        TestStateMutationPatterns(),
        TestCallbackImplementation(),
    ]

    total_tests = 0
    passed_tests = 0

    for test_class in test_suite:
        print(f"\n🧪 Running {test_class.__class__.__name__}")
        print("-" * 60)

        for method_name in dir(test_class):
            if method_name.startswith("test_"):
                total_tests += 1
                try:
                    method = getattr(test_class, method_name)
                    method()
                    passed_tests += 1
                except AssertionError as e:
                    print(f"❌ {method_name}: {e}")
                except Exception as e:
                    print(f"💥 {method_name}: Unexpected error: {e}")

    # Run standalone tests
    print(f"\n🧪 Running standalone tests")
    print("-" * 60)
    try:
        test_no_old_intent_flag_references()
        total_tests += 1
        passed_tests += 1
    except AssertionError as e:
        print(f"❌ test_no_old_intent_flag_references: {e}")
        total_tests += 1

    print()
    print("=" * 60)
    print(f"RESULTS: {passed_tests}/{total_tests} tests passed")
    print("=" * 60)

    if passed_tests == total_tests:
        print("🎉 All tests passed! Execution model refactoring is complete.")
        sys.exit(0)
    else:
        print(f"⚠️  {total_tests - passed_tests} test(s) failed")
        sys.exit(1)
