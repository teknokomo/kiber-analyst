from __future__ import annotations

import os
from pathlib import Path


def config_dir() -> Path:
    return Path(os.environ.get("KIBER_ANALYST_CONFIG_DIR", Path.home() / ".config/kiber-analyst"))


def data_dir() -> Path:
    return Path(os.environ.get("KIBER_ANALYST_DATA_ROOT", Path.home() / ".local/share/kiber-analyst"))


def runtime_dir() -> Path:
    base = Path(os.environ.get("XDG_RUNTIME_DIR", Path.home() / ".local/run"))
    return base / "kiber-analyst"


def config_path(name: str) -> Path:
    return config_dir() / name


def data_path(*parts: str) -> Path:
    return data_dir().joinpath(*parts)


def runtime_path(*parts: str) -> Path:
    return runtime_dir().joinpath(*parts)
