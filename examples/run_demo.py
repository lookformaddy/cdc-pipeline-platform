"""End-to-end demo runner.

Runs the same flow as ``python -m cdc_pipeline demo`` but from the
examples directory, so the demo is runnable without installing the package
(the script adds ``src/`` to ``sys.path`` itself).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cdc_pipeline.cli import run_demo  # noqa: E402
from cdc_pipeline.config import Settings  # noqa: E402


def main() -> int:
    return run_demo(Settings())


if __name__ == "__main__":
    raise SystemExit(main())
