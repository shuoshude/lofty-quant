"""因子元数据注册和查询。"""

from __future__ import annotations

from collections.abc import Iterable

from quant.features.base import FactorCategory, FactorMetadata


class FactorRegistry:
    """维护彼此隔离的一组因子元数据。"""

    def __init__(self, factors: Iterable[FactorMetadata] = ()) -> None:
        """使用可选的初始元数据创建独立注册表。"""
        self._factors: dict[tuple[str, str], FactorMetadata] = {}
        for metadata in factors:
            self.register(metadata)

    def register(self, metadata: FactorMetadata) -> None:
        """注册一个因子元数据。"""
        key = (metadata.name, metadata.version)
        if key in self._factors:
            raise ValueError(f"因子已注册: name={metadata.name}, version={metadata.version}")
        self._factors[key] = metadata

    def get(self, name: str, version: str = "v1") -> FactorMetadata:
        """按稳定名称和版本查询因子元数据。"""
        key = (name, version)
        try:
            return self._factors[key]
        except KeyError as exc:
            raise KeyError(f"未注册因子: name={name}, version={version}") from exc

    def list_factors(
        self,
        category: FactorCategory | None = None,
    ) -> tuple[FactorMetadata, ...]:
        """按稳定顺序列出全部或指定类别的因子元数据。"""
        factors = (
            metadata
            for metadata in self._factors.values()
            if category is None or metadata.category == category
        )
        return tuple(sorted(factors, key=lambda metadata: (metadata.name, metadata.version)))


def build_default_registry() -> FactorRegistry:
    """创建包含项目内置因子的独立注册表。"""
    from quant.features.alternative import ALTERNATIVE_FACTORS
    from quant.features.technical import TECHNICAL_FACTORS

    return FactorRegistry((*TECHNICAL_FACTORS, *ALTERNATIVE_FACTORS))
