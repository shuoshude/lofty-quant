from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ETLTask:
    """一次 ETL 任务的最小描述。"""

    dataset: str
    source: str
    start_date: date
    end_date: date
    exchange: str | None = None
    force: bool = False
    dry_run: bool = False