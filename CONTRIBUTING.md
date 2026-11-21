# Contributing

1. Use Python 3.12+.
2. Install the development extras with `pip install -e '.[test,db,embedding,mcp]'`.
3. Keep fixtures synthetic and use temporary databases only.
4. Run `pytest -q`, `ruff check .`, `python -m compileall -q src`, and `python scripts/public_release_scan.py`.
5. Do not add credentials, real exports, production identifiers, private deployment paths, or runtime databases.
6. Keep MCP read-only and preserve the broker's allowlist and path-validation boundaries.

Please describe user-visible behavior, compatibility impact, and any database migration in your change.
