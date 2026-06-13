from pathlib import Path

from quant.config import load_config
from quant.utils import get_project_root, resolve_log_dir, resolve_path


def test_get_project_root_finds_repository_root() -> None:
    project_root = get_project_root()

    assert project_root.name == "lofty-quant"
    assert (project_root / "pyproject.toml").exists()


def test_resolve_log_dir_resolves_relative_path_from_project_root(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    write_settings(config_dir / "settings.toml")
    config = load_config(config_dir=config_dir)

    assert resolve_log_dir("custom-log", config) == get_project_root() / "custom-log"


def test_resolve_log_dir_keeps_absolute_path(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    write_settings(config_dir / "settings.toml")
    config = load_config(config_dir=config_dir)

    assert resolve_log_dir(tmp_path / "absolute-log", config) == tmp_path / "absolute-log"


def test_resolve_path_resolves_relative_path_from_base_dir(tmp_path: Path) -> None:
    assert resolve_path("relative/path", tmp_path) == tmp_path / "relative" / "path"


def test_resolve_path_keeps_absolute_path(tmp_path: Path) -> None:
    absolute_path = tmp_path / "absolute" / "path"

    assert resolve_path(absolute_path, get_project_root()) == absolute_path


def test_resolve_path_expands_home_dir() -> None:
    assert (
        resolve_path("~/lofty-quant-test", get_project_root())
        == (Path.home() / "lofty-quant-test").resolve()
    )


def write_settings(path: Path) -> None:
    path.write_text(
        """
[project]
name = "test"

[paths]
raw_dir = "data/raw"
processed_dir = "data/processed"
database_path = "data/db/test.duckdb"
notebooks_dir = "notebooks"
log_dir = "log"
""",
        encoding="utf-8",
    )
