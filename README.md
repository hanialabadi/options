# Options AI Pipeline

## Project Governance: Phase Function Interface Contract

To ensure interface stability and prevent silent drift between CLI and Dashboard execution, all Phase functions must adhere to the following structural rules:

1.  **Positional Data Inputs**: Accept primary data inputs (e.g., `input_path`, `df`) positionally.
2.  **Keyword-Only Configuration**: Accept all configuration arguments (e.g., `save_snapshot`, `enable_logging`) ONLY as keyword-only arguments (using the `*` separator).
3.  **No Positional Booleans**: Never allow boolean flags to be passed positionally.
4.  **Interface Parity**: CLI and UI components must call Phase functions identically, using explicit keywords for all configuration.
5.  **Signature Enforcement**: Use `inspect.signature` assertions at the module level to enforce these contracts and prevent accidental regressions.

Example:
```python
def phase_n_function(data_input, *, config_arg=True):
    ...
