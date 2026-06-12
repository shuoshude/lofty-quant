"""项目配置统一加载入口。"""

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
    """项目元信息。"""

    model_config = ConfigDict(frozen=True)

    name: str = "lofty-quant"
    timezone: str = "Asia/Shanghai"


class PathsConfig(BaseModel):
    """项目使用的文件系统路径。"""

    model_config = ConfigDict(frozen=True)

    raw_dir: Path
    processed_dir: Path
    database_path: Path
    notebooks_dir: Path
    log_dir: Path

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any], base_dir: Path) -> Self:
        """创建路径配置, 并按项目根目录解析相对路径。"""
        resolved = {key: _resolve_path(Path(value), base_dir) for key, value in raw.items()}
        return cls.model_validate(resolved)


class MarketConfig(BaseModel):
    """A 股市场约定。"""

    model_config = ConfigDict(frozen=True)

    code_suffixes: tuple[str, ...] = (".SZ", ".SH", ".BJ")

    @field_validator("code_suffixes")
    @classmethod
    def validate_code_suffixes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """校验证券代码后缀是否符合交易所格式。"""
        if not value:
            raise ValueError("code_suffixes 不能为空")
        invalid = [suffix for suffix in value if not suffix.startswith(".")]
        if invalid:
            raise ValueError(f"无效的代码后缀: {invalid}")
        return value


class TradingConfig(BaseModel):
    """交易费用假设。"""

    model_config = ConfigDict(frozen=True)

    stamp_tax_rate: float = Field(default=0.001, ge=0)
    commission_rate: float = Field(default=0.00025, ge=0)
    transfer_fee_rate: float = Field(default=0.00002, ge=0)
    min_commission: float = Field(default=5.0, ge=0)


class BacktestConfig(BaseModel):
    """回测默认参数。"""

    model_config = ConfigDict(frozen=True)

    initial_cash: float = Field(default=1_000_000.0, gt=0)
    benchmark: str = "000300.SH"


class SecretsConfig(BaseModel):
    """从环境变量读取的机密配置。"""

    model_config = ConfigDict(frozen=True)

    tushare_token: str | None = None
    akshare_token: str | None = None


class QuantConfig(BaseModel):
    """项目顶层配置。"""

    model_config = ConfigDict(frozen=True)

    project: ProjectConfig
    paths: PathsConfig
    market: MarketConfig = Field(default_factory=MarketConfig)
    trading: TradingConfig = Field(default_factory=TradingConfig)
    backtest: BacktestConfig = Field(default_factory=BacktestConfig)
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any], base_dir: Path) -> Self:
        """根据合并后的 TOML 配置和环境变量机密构建配置对象。"""
        if "paths" not in raw or not isinstance(raw["paths"], Mapping):
            raise ValueError("配置必须包含 paths 映射")

        data = dict(raw)
        data["paths"] = PathsConfig.from_raw(raw["paths"], base_dir)
        return cls.model_validate(data)


def load_config(
    *,
    config_dir: Path | str | None = None,
    environment: str | None = None,
) -> QuantConfig:
    """加载多层 TOML 配置并校验最终配置对象。"""
    project_root = get_project_root()
    settings_dir = _resolve_settings_dir(config_dir, project_root)
    settings_files = _settings_files(settings_dir, environment)

    raw = _load_settings_files(settings_files)
    raw["secrets"] = _load_secret_environment()

    try:
        return QuantConfig.from_mapping(raw, project_root)
    except ValidationError as exc:
        raise ValueError(f"配置文件无效: {settings_files}") from exc


# 获取项目根目录
def get_project_root(start: Path | None = None) -> Path:
    """向上查找包含 pyproject.toml 的项目根目录。"""
    current = (start or Path.cwd()).expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("无法找到包含 pyproject.toml 的项目根目录")


def _load_settings_files(settings_files: list[Path]) -> dict[str, Any]:
    """按顺序加载 TOML 配置文件, 后面的文件覆盖前面的文件。"""
    existing_files = [path for path in settings_files if path.exists()]
    if not existing_files:
        raise FileNotFoundError(f"未找到配置文件: {settings_files}")

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
    """按覆盖顺序返回配置文件列表。"""
    selected_environment = environment or _read_environment_name()
    files = [settings_dir / DEFAULT_SETTINGS_FILE]
    if selected_environment and selected_environment != "default":
        files.append(settings_dir / f"settings.{selected_environment}.toml")
    files.append(settings_dir / LOCAL_SETTINGS_FILE)
    return files


def _read_environment_name() -> str:
    """读取可选的配置环境名称。"""
    import os

    return os.getenv(ENVIRONMENT_VARIABLE, "default").strip().lower()


def _load_secret_environment() -> dict[str, str]:
    """加载项目前缀下的机密环境变量。"""
    import os

    secrets: dict[str, str] = {}
    for key, value in os.environ.items():
        if not key.startswith(SECRETS_ENV_PREFIX) or value == "":
            continue
        secret_key = key.removeprefix(SECRETS_ENV_PREFIX).lower()
        secrets[secret_key] = value
    return secrets


def _resolve_settings_dir(config_dir: Path | str | None, project_root: Path) -> Path:
    """解析配置目录。"""
    if config_dir is None:
        return project_root / CONFIG_DIR

    path = Path(config_dir).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (project_root / path).resolve()


def _resolve_path(path: Path, base_dir: Path) -> Path:
    """按项目根目录解析路径。"""
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded.resolve()
    return (base_dir / expanded).resolve()
