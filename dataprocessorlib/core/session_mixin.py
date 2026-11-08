"""Session + logger + param validation shared by inputs and targets (processors get it from BaseProcessor)."""
from __future__ import annotations
import os
from contextlib import contextmanager
from typing import Any, Optional

from dataprocessorlib.utils.logger import get_json_logger


class SessionMixin:
    def __init__(self, session: Optional[Any] = None) -> None:
        self.session = session
        self._logger = None

    @property
    def logger(self):
        if self._logger is None:
            self._logger = get_json_logger(name=self.__class__.__module__,
                                           level=os.environ.get("log_level", "INFO").upper())
        return self._logger

    @logger.setter
    def logger(self, value) -> None:
        self._logger = value

    def _job_name(self, kwargs: dict) -> str:
        return kwargs.get("job_name", f"{self.__class__.__name__}Job")

    def _validate_required_params(self, params: dict, required: list) -> None:
        missing = [k for k in required if k not in params or params[k] is None]
        if missing:
            raise ValueError(f"{self.__class__.__name__} requires parameters: {missing}")

    @contextmanager
    def transaction(self):
        """BEGIN ... COMMIT; ROLLBACK (and re-raise) if anything inside fails. DML only inside:
        DDL, including cache_result(), may implicitly commit, so materialise dataframes first."""
        self.session.sql("BEGIN").collect()
        try:
            yield
        except Exception:
            self.session.sql("ROLLBACK").collect()
            raise
        else:
            self.session.sql("COMMIT").collect()
