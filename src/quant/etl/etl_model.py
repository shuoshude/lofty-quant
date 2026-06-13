"""ETL 任务模型。"""

from datetime import date

from pydantic import BaseModel, ConfigDict


class ETLTask(BaseModel):
    """一次 ETL 任务的最小描述。"""

    model_config = ConfigDict(frozen=True)

    dataset: str
    source: str
    start_date: date
    end_date: date
    exchange: str | None = None
    force: bool = False
    dry_run: bool = False
