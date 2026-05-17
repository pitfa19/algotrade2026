"""Makes ``bots/`` and ``bots/archive/`` importable when tests are collected by
either pytest or stdlib unittest. The repo's ``conftest.py`` does the same for
pytest; this package init handles the unittest discovery path.
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
for _p in (_REPO_ROOT / "bots", _REPO_ROOT / "bots" / "archive"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
