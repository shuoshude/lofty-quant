from pathlib import Path

import pytest

from quant.config import get_project_root, load_config


def test_load_default_config_resolves_paths() -> None:
    config = load_config()
    project_root = get_project_root()

    assert config.project.name == "lofty-quant"
    assert config.project.timezone == "Asia/Shanghai"
    assert config.paths.raw_dir == project_root / "data" / "raw"
    assert config.paths.processed_dir == project_root / "data" / "processed"
    assert config.paths.database_path == project_root / "data" / "db" / "quant.duckdb"
    assert config.market.code_suffixes == (".SZ", ".SH", ".BJ")


def test_load_config_applies_environment_then_local_overrides(tmp_path: Path) -> None:
    write_settings(
        tmp_path / "settings.toml",
        """
[project]
name = "base"

[paths]
raw_dir = "raw"
processed_dir = "processed"
database_path = "db/base.duckdb"
notebooks_dir = "notebooks"

[backtest]
initial_cash = 100.0
benchmark = "000300.SH"
""",
    )
    write_settings(
        tmp_path / "settings.research.toml",
        """
[project]
name = "research"

[backtest]
initial_cash = 200.0
""",
    )
    write_settings(
        tmp_path / "settings.local.toml",
        """
[project]
name = "local"

[paths]
raw_dir = "/tmp/local/raw"
""",
    )

    config = load_config(config_dir=tmp_path, environment="research")

    assert config.project.name == "local"
    assert config.backtest.initial_cash == 200.0
    assert config.paths.raw_dir == Path("/tmp/local/raw").resolve()
    assert config.paths.database_path == get_project_root() / "db" / "base.duckdb"


def test_load_config_reads_secrets_from_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_minimal_settings(tmp_path)
    monkeypatch.setenv("LOFTY_QUANT__SECRETS__TUSHARE_TOKEN", "token-from-env")
    monkeypatch.setenv("LOFTY_QUANT__PROJECT__NAME", "ignored")

    config = load_config(config_dir=tmp_path)

    assert config.project.name == "custom"
    assert config.secrets.tushare_token == "token-from-env"


def test_load_config_selects_environment_from_env_var(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_minimal_settings(tmp_path)
    write_settings(
        tmp_path / "settings.paper.toml",
        """
[backtest]
benchmark = "000905.SH"
""",
    )
    monkeypatch.setenv("LOFTY_QUANT_ENV", "paper")

    config = load_config(config_dir=tmp_path)

    assert config.backtest.benchmark == "000905.SH"


def test_load_config_rejects_invalid_fee(tmp_path: Path) -> None:
    write_settings(
        tmp_path / "settings.toml",
        """
[project]
name = "invalid"

[paths]
raw_dir = "raw"
processed_dir = "processed"
database_path = "db/quant.duckdb"
notebooks_dir = "notebooks"

[trading]
commission_rate = -0.1
""",
    )

    with pytest.raises(ValueError, match="invalid config"):
        load_config(config_dir=tmp_path)


def test_load_config_rejects_missing_settings_dir(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no settings files"):
        load_config(config_dir=tmp_path)


def write_minimal_settings(config_dir: Path) -> None:
    write_settings(
        config_dir / "settings.toml",
        """
[project]
name = "custom"

[paths]
raw_dir = "raw"
processed_dir = "processed"
database_path = "db/quant.duckdb"
notebooks_dir = "notebooks"
""",
    )


def write_settings(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
