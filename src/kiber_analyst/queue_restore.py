from __future__ import annotations

import argparse
import os
from pathlib import Path

from kiber_analyst.jobqueue import DEFAULT_DATA_ROOT, DEFAULT_UUID, JobStore


def main() -> int:
    parser=argparse.ArgumentParser(prog="kiber-analyst-queue-restore")
    parser.add_argument("backup",type=Path)
    args=parser.parse_args()
    root=Path(os.environ.get("KIBER_ANALYST_DATA_ROOT",str(DEFAULT_DATA_ROOT)))
    expected=os.environ.get("KIBER_ANALYST_DATA_UUID",DEFAULT_UUID)
    store=JobStore(root,expected_uuid=expected)
    print(store.restore_from_backup(args.backup))
    return 0


if __name__=="__main__":
    raise SystemExit(main())
