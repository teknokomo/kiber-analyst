# Project identity

The public product name is **Kiber-analyst**. The distribution and primary Python package use the following names:

| Purpose | Name |
| --- | --- |
| Python distribution and CLI | `kiber-analyst` |
| Core Python package | `kiber_analyst` |
| MCP Python package | `kiber_analyst_mcp` |
| Configuration namespace | `KIBER_ANALYST_*` |

Configuration and data locations follow XDG user directories by default and can be overridden with documented environment variables. Deployment-specific paths, service accounts, and runtime units are intentionally outside this source package.
