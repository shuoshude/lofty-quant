"""Central project configuration loading."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self

from dynaconf import Dynaconf
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

CONFIG_DIR = "config"
DEFAULT_SETTINGS_FILE = "settings.toml"
LOCAL_SETTINGS_FILE = "settings.local.toml"
ENVIRONMENT_VARIABLE = "LOFTY_QUANT_ENV"
SECRETS_ENV_PREFIX = "LOFTY_QUANT__SECRETS__"
TOP_LEVEL_SECTIONS = ("project", "paths", "market", "trading", "backtest")


class ProjectConfig(BaseModel):
    """Project metadata."""

    model_config = ConfigDict(frozen=True)

    name: str = "lofty-quant"
    timezone: str = "Asia/Shanghai"


class PathsConfig(BaseModel):
    """Filesystem paths used by the project."""

    model_config = ConfigDict(frozen=True)

    raw_dir: Path
    processed_dir: Path
    database_path: Path
    notebooks_dir: Path
    log_dir: Path

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any], base_dir: Path) -> Self:
        """Create path config and resolve relative paths from the project root."""
        resolved = {key: _resolve_path(Path(value), base_dir) for key, value in raw.items()}
        return cls.model_validate(resolved)


class MarketConfig(BaseModel):
    """A-share market conventions."""

    model_config = ConfigDict(frozen=True)

    code_suffixes: tuple[str, ...] = (".SZ", ".SH", ".BJ")

    @field_validator("code_suffixes")
    @classmethod
    def validate_code_suffixes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Ensure stock code suffixes use the expected exchange style."""
        if not value:
            raise ValueError("code_suffixes cannot be empty")
        invalid = [suffix for suffix in value if not suffix.startswith(".")]
        if invalid:
            raise ValueError(f"invalid code suffixes: {invalid}")
        return value


class TradingConfig(BaseModel):
    """Trading fee assumptions."""

    model_config = ConfigDict(frozen=True)

    stamp_tax_rate: float = Field(default=0.001, ge=0)
    commission_rate: float = Field(default=0.00025, ge=0)
    transfer_fee_rate: float = Field(default=0.00002, ge=0)
    min_commission: float = Field(default=5.0, ge=0)


class BacktestConfig(BaseModel):
    """Backtest defaults."""

    model_config = ConfigDict(frozen=True)

    initial_cash: float = Field(default=1_000_000.0, gt=0)
    benchmark: str = "000300.SH"


class SecretsConfig(BaseModel):
    """Secret values loaded from environment variables."""

    model_config = ConfigDict(frozen=True)

    tushare_token: str | None = None
    akshare_token: str | None = None


class QuantConfig(BaseModel):
    """Top-level project configuration."""

    model_config = ConfigDict(frozen=True)

    project: ProjectConfig
    paths: PathsConfig
    market: MarketConfig = Field(default_factory=MarketConfig)
    trading: TradingConfig = Field(default_factory=TradingConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], base_dir: Path) -> Self:
        """Build config from merged TOML settings and secret environment variables."""
        if "paths" not in raw or not isinstance(raw["paths"], Mapping):
            raise ValueError("config must contain a paths mapping")

        data = dict(raw)
        data["paths"] = PathsConfig.from_raw(raw["paths"], base_dir)
        return cls.model_validate(data)


def load_config(
    *,
    config_dir: Path | str | None = None,
    environment: str | None = None,
) -> QuantConfig:
    """Load layered TOML config and validate the resulting settings object."""
    project_root = get_project_root()
    settings_dir = _resolve_settings_dir(config_dir, project_root)
    settings_files = _settings_files(settings_dir, environment)

    raw = _load_settings_files(settings_files)
    raw["secrets"] = _load_secret_environment()

    try:
        return QuantConfig.from_mapping(raw, project_root)
    except ValidationError as exc:
        raise ValueError(f"invalid config from settings files: {settings_files}") from exc


# 获取项目根目录
def get_project_root(start: Path | None = None) -> Path:
    """Find the repository root by walking upward until pyproject.toml is found."""
    current = (start or Path.cwd()).expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("could not find project root containing pyproject.toml")


def _load_settings_files(settings_files: list[Path]) -> dict[str, Any]:
    """Load TOML settings files with later files overriding earlier ones."""
    existing_files = [path for path in settings_files if path.exists()]
    if not existing_files:
        raise FileNotFoundError(f"no settings files found: {settings_files}")

    settings = Dynaconf(
        environments=False,
        envvar_prefix=False,
        load_dotenv=False,
        settings_files=[str(path) for path in existing_files],
        core_loaders=["TOML"],
        merge_enabled=True,
    )
    data: dict[str, Any] = {}
    for section in TOP_LEVEL_SECTIONS:
        value = settings.get(section.upper())
        if value is not None:
            data[section] = dict(value) if isinstance(value, Mapping) else value
    return data


def _settings_files(settings_dir: Path, environment: str | None) -> list[Path]:
    """Return settings files in override order."""
    selected_environment = environment or _read_environment_name()
    files = [settings_dir / DEFAULT_SETTINGS_FILE]
    if selected_environment and selected_environment != "default":
        files.append(settings_dir / f"settings.{selected_environment}.toml")
    files.append(settings_dir / LOCAL_SETTINGS_FILE)
    return files


def _read_environment_name() -> str:
    """Read the optional config environment selector."""
    import os

    return os.getenv(ENVIRONMENT_VARIABLE, "default").strip().lower()


def _load_secret_environment() -> dict[str, str]:
    """Load secret environment variables under the project prefix."""
    import os

    secrets: dict[str, str] = {}
    for key, value in os.environ.items():
        if not key.startswith(SECRETS_ENV_PREFIX) or value == "":
            continue
        secret_key = key.removeprefix(SECRETS_ENV_PREFIX).lower()
        secrets[secret_key] = value
    return secrets


def _resolve_settings_dir(config_dir: Path | str | None, project_root: Path) -> Path:
    """Resolve the settings directory."""
    if config_dir is None:
        return project_root / CONFIG_DIR

    path = Path(config_dir).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (project_root / path).resolve()


def _resolve_path(path: Path, base_dir: Path) -> Path:
    """Resolve a path relative to the project root."""
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (base_dir / expanded).resolve()
