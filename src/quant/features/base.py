"""因子模块的基础领域模型。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FactorCategory = Literal["technical", "fundamental", "alternative"]


@dataclass(frozen=True, slots=True)
class FactorMetadata:
    """描述一个因子的稳定身份、计算依赖和研究方向。"""

    name: str  # 稳定因子名称,不包含版本号
    version: str  # 因子计算定义版本
    category: FactorCategory  # 因子所属研究类别
    lookback_days: int  # 计算前需要读取的历史交易日数
    required_fields: tuple[str, ...]  # 计算因子所需的输入字段
    min_periods: int  # 产生有效因子值所需的最少观测数
    higher_is_better: bool | None  # 因子值越大是否代表预期收益越高
    description: str  # 因子公式和研究含义的简短说明

    def __post_init__(self) -> None:
        """拒绝无法用于注册和计算的元数据。"""
        if not self.name.strip():
            raise ValueError("因子名称不能为空")
        if not self.version.strip():
            raise ValueError("因子版本不能为空")
        if self.category not in ("technical", "fundamental", "alternative"):
            raise ValueError(f"因子类别无效: category={self.category}")
        if self.lookback_days < 0:
            raise ValueError("回看交易日数不能小于 0")
        if not self.required_fields:
            raise ValueError("输入字段不能为空")
        if any(not field.strip() for field in self.required_fields):
            raise ValueError("输入字段名称不能为空")
        if len(set(self.required_fields)) != len(self.required_fields):
            raise ValueError("输入字段不能重复")
        if self.min_periods < 1:
            raise ValueError("最少观测数不能小于 1")
        if not self.description.strip():
            raise ValueError("因子说明不能为空")
