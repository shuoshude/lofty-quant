"""因子研究能力的公共接口。"""

from quant.features.base import FactorCategory, FactorMetadata
from quant.features.labels import compute_forward_return_5d
from quant.features.pipeline import FactorRunSummary, run_factor_pipeline
from quant.features.processing import FactorProcessor
from quant.features.registry import FactorRegistry, build_default_registry
from quant.features.storage import write_factor_results
from quant.features.technical import compute_return_5d
from quant.features.validation import (
    FactorDateCoverage,
    FactorValidationReport,
    run_factor_validation,
    validate_factor,
)

__all__ = [
    "FactorCategory",
    "FactorDateCoverage",
    "FactorMetadata",
    "FactorProcessor",
    "FactorRegistry",
    "FactorRunSummary",
    "FactorValidationReport",
    "build_default_registry",
    "compute_forward_return_5d",
    "compute_return_5d",
    "run_factor_pipeline",
    "run_factor_validation",
    "validate_factor",
    "write_factor_results",
]
