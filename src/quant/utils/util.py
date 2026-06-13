from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quant.config import QuantConfig


def get_project_root(start: Path | None = None) -> Path:
    """向上查找包含 pyproject.toml 的项目根目录。"""
    current = (start or Path.cwd()).expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("无法找到包含 pyproject.toml 的项目根目录")


def resolve_path(path: Path | str, base_dir: Path) -> Path:
    """按给定基准目录解析路径。"""
    expanded = Path(path).expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (base_dir / expanded).resolve()


def resolve_log_dir(log_dir: Path | str | None, config: QuantConfig) -> Path:
    """解析运行日志目录。"""
    if log_dir is None:
        return config.paths.log_dir

    return resolve_path(log_dir, get_project_root())
