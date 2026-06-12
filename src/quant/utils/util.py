from pathlib import Path

from quant.config import QuantConfig


def get_project_root(start: Path | None = None) -> Path:
    """向上查找包含 pyproject.toml 的项目根目录。"""
    current = (start or Path.cwd()).expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("无法找到包含 pyproject.toml 的项目根目录")


def resolve_log_dir(log_dir: Path | str | None, config: QuantConfig) -> Path:
    """解析运行日志目录。"""
    if log_dir is None:
        return config.paths.log_dir

    path = Path(log_dir).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (get_project_root() / path).resolve()
