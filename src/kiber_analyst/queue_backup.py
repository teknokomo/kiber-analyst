from __future__ import annotations

from kiber_analyst.jobapi import api_call


def main() -> int:
    result=api_call("backup")
    print(result["backup_path"])
    return 0


if __name__=="__main__":
    raise SystemExit(main())
