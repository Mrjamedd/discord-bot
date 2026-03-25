from __future__ import annotations

import sys
from pathlib import Path

from deployment_bootstrap import bootstrap_environment


try:
    bootstrap_environment()
except RuntimeError as exc:
    print(f"Bootstrap error: {exc}", file=sys.stderr)
    raise SystemExit(1)

PROJECT_MODULE_DIR = Path(__file__).resolve().parent / "Bot Main file and utlities"
if str(PROJECT_MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_MODULE_DIR))

from main import main


if __name__ == "__main__":
    raise SystemExit(main())
