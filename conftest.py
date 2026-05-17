"""Pytest auto-loads this; adds bots/ and bots/archive/ to sys.path so tests can import bot modules by name."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _p in (_ROOT / "bots", _ROOT / "bots" / "archive"):
    sys.path.insert(0, str(_p))
