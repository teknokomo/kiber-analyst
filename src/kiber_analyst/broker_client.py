"""Forward the fixed restricted SSH commands over the broker socket."""
import json
import os
import socket

from kiber_analyst.config_paths import runtime_path

MAX_SEARCH_TOKENS = 32
MAX_ARGUMENT_CHARS = 180


def normalize_args(args):
    """Keep a multiword search query in one RPC argument after shell parsing."""
    if (not isinstance(args, (list, tuple)) or not args
            or any(not isinstance(value, str) or len(value) > MAX_ARGUMENT_CHARS for value in args)):
        raise ValueError('INVALID_REQUEST')
    normalized = list(args)
    if normalized[0] == 'search' and len(normalized) > 4:
        if len(normalized) > MAX_SEARCH_TOKENS:
            raise ValueError('INVALID_REQUEST')
        normalized = [normalized[0], normalized[1], ' '.join(normalized[2:-1]), normalized[-1]]
    if len(normalized) > 4:
        raise ValueError('INVALID_REQUEST')
    return normalized


def handle(args):
    try:
        normalized = normalize_args(args)
    except ValueError:
        print('{"ok":false,"error_code":"INVALID_REQUEST"}')
        raise SystemExit(1)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(1800)
            client.connect(os.environ.get("KIBER_ANALYST_BROKER_SOCKET", str(runtime_path("broker.sock"))))
            client.sendall((json.dumps(normalized) + '\n').encode())
            with client.makefile('rb') as response:
                raw = response.readline(65537)
        if len(raw) > 65536 or not raw.endswith(b'\n'):
            raise ValueError
        result = json.loads(raw)
    except (OSError, ValueError):
        result = {'ok': False, 'error_code': 'BROKER_UNAVAILABLE'}
    print(json.dumps(result))
    if not result.get('ok'):
        raise SystemExit(1)


def main() -> None:
    import sys

    handle(sys.argv[1:])
