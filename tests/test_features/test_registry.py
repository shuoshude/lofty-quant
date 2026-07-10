from dataclasses import FrozenInstanceError

import pytest

from quant.features import FactorMetadata, FactorRegistry, build_default_registry


def make_metadata(
    *,
    name: str = "custom_factor",
    version: str = "v1",
) -> FactorMetadata:
    """构造注册表测试使用的有效因子元数据。"""
    return FactorMetadata(
        name=name,
        version=version,
        category="technical",
        lookback_days=1,
        required_fields=("hfq_close",),
        min_periods=1,
        higher_is_better=None,
        description="测试因子。",
    )


def test_default_registry_gets_factor_by_name() -> None:
    """默认注册表可以按名称和默认版本查询因子。"""
    registry = build_default_registry()

    metadata = registry.get("return_5d")

    assert metadata.name == "return_5d"
    assert metadata.version == "v1"
    assert metadata.lookback_days == 5
    assert metadata.required_fields == ("hfq_close",)
    assert metadata.higher_is_better is False


def test_default_registry_lists_all_factors_in_stable_order() -> None:
    """默认注册表稳定列出完整的内置因子集合。"""
    registry = build_default_registry()

    factors = registry.list_factors()

    assert [metadata.name for metadata in factors] == [
        "amihud_20d",
        "log_amount_mean_20d",
        "momentum_20d",
        "return_5d",
        "volatility_20d",
    ]
    assert [
        (
            metadata.name,
            metadata.category,
            metadata.lookback_days,
            metadata.min_periods,
            metadata.required_fields,
            metadata.higher_is_better,
        )
        for metadata in factors
    ] == [
        ("amihud_20d", "alternative", 20, 20, ("hfq_close", "amount"), False),
        ("log_amount_mean_20d", "alternative", 20, 20, ("amount",), None),
        ("momentum_20d", "technical", 20, 21, ("hfq_close",), True),
        ("return_5d", "technical", 5, 6, ("hfq_close",), False),
        ("volatility_20d", "technical", 20, 20, ("hfq_close",), False),
    ]


def test_factor_metadata_is_immutable() -> None:
    """因子元数据创建后不可修改。"""
    metadata = make_metadata()

    with pytest.raises(FrozenInstanceError):
        metadata.name = "changed"  # type: ignore[misc]


def test_registry_rejects_duplicate_name_and_version() -> None:
    """注册表拒绝重复的因子名称和版本组合。"""
    metadata = make_metadata()
    registry = FactorRegistry((metadata,))

    with pytest.raises(
        ValueError,
        match=r"因子已注册: name=custom_factor, version=v1",
    ):
        registry.register(metadata)


def test_registry_allows_same_name_with_different_versions() -> None:
    """注册表允许同名因子的不同版本共存。"""
    registry = FactorRegistry()
    v1 = make_metadata(version="v1")
    v2 = make_metadata(version="v2")

    registry.register(v1)
    registry.register(v2)

    assert registry.get("custom_factor") == v1
    assert registry.get("custom_factor", version="v2") == v2


def test_registry_reports_unknown_factor() -> None:
    """查询未知因子时返回包含查询条件的错误。"""
    registry = FactorRegistry()

    with pytest.raises(
        KeyError,
        match=r"未注册因子: name=unknown, version=v2",
    ):
        registry.get("unknown", version="v2")


def test_registry_filters_factors_by_category() -> None:
    """因子列表可以按类别过滤。"""
    registry = build_default_registry()

    factors = registry.list_factors(category="alternative")

    assert [metadata.name for metadata in factors] == [
        "amihud_20d",
        "log_amount_mean_20d",
    ]


def test_default_registries_are_isolated() -> None:
    """每个默认注册表实例拥有独立的可变状态。"""
    first_registry = build_default_registry()
    second_registry = build_default_registry()
    first_registry.register(make_metadata())

    assert first_registry.get("custom_factor").name == "custom_factor"
    with pytest.raises(KeyError, match=r"未注册因子: name=custom_factor, version=v1"):
        second_registry.get("custom_factor")


@pytest.mark.parametrize(
    ("changes", "error_message"),
    [
        ({"name": " "}, "因子名称不能为空"),
        ({"version": ""}, "因子版本不能为空"),
        ({"category": ""}, "因子类别无效"),
        ({"category": "unknown"}, "因子类别无效"),
        ({"lookback_days": -1}, "回看交易日数不能小于 0"),
        ({"required_fields": ()}, "输入字段不能为空"),
        ({"required_fields": ("hfq_close", " ")}, "输入字段名称不能为空"),
        (
            {"required_fields": ("hfq_close", "hfq_close")},
            "输入字段不能重复",
        ),
        ({"min_periods": 0}, "最少观测数不能小于 1"),
        ({"description": " "}, "因子说明不能为空"),
    ],
)
def test_factor_metadata_rejects_invalid_values(
    changes: dict[str, object],
    error_message: str,
) -> None:
    """因子元数据在创建时拒绝无效字段。"""
    values: dict[str, object] = {
        "name": "custom_factor",
        "version": "v1",
        "category": "technical",
        "lookback_days": 1,
        "required_fields": ("hfq_close",),
        "min_periods": 1,
        "higher_is_better": None,
        "description": "测试因子。",
    }
    values.update(changes)

    with pytest.raises(ValueError, match=error_message):
        FactorMetadata(**values)  # type: ignore[arg-type]
