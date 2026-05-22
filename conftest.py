import sys
import os

# The Frappe app convention places an empty __init__.py at the repo root.
# pytest sees this, registers the PARENT dir as the import root, and caches
# the root directory itself as the `tap_ai` package in sys.modules —
# BEFORE test modules are imported. The cached entry has no `services` subpackage.
# Fix: forcibly evict the stale sys.modules entry and ensure the repo root (which
# contains the real inner tap_ai/ package) is first on sys.path.
_repo_root = os.path.dirname(os.path.abspath(__file__))
_apps_parent = os.path.dirname(_repo_root)

if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

if _apps_parent in sys.path:
    sys.path.remove(_apps_parent)

# Evict any stale tap_ai cache that pytest may have registered from the root __init__.py
_stale = [k for k in sys.modules if k == "tap_ai" or k.startswith("tap_ai.")]
for k in _stale:
    del sys.modules[k]
