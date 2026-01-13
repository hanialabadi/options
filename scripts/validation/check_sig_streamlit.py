import streamlit as st
import inspect
import core.phase1_clean
from core.phase1_clean import phase1_load_and_clean_positions

st.write(f"File: {inspect.getfile(phase1_load_and_clean_positions)}")
st.write(f"Signature: {inspect.signature(phase1_load_and_clean_positions)}")

try:
    # Try to call it with input_path
    from pathlib import Path
    phase1_load_and_clean_positions(input_path=Path("non_existent.csv"), save_snapshot=False)
    st.success("Call with input_path succeeded")
except TypeError as e:
    st.error(f"Call with input_path failed: {e}")
except Exception as e:
    st.info(f"Call failed with other error: {type(e).__name__}: {e}")
