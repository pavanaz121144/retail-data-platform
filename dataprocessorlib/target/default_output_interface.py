from dataprocessorlib.core.base import OutputInterface
from dataprocessorlib.core.registry import resolve_class
from dataprocessorlib.core.session_mixin import SessionMixin
from dataprocessorlib.core.utils import ExceptionHandler, select_columns


class DefaultOutputInterface(SessionMixin, OutputInterface):
    """Plain write: optional truncate before/after, then the persistence strategy."""

    @ExceptionHandler.handle_exception
    def persist(self, dfs, target_name, **kwargs) -> bool:
        job = self._job_name(kwargs)
        self._validate_required_params(kwargs, ["input_key"])
        if not target_name:
            raise ValueError("target_name is required")
        if kwargs["input_key"] not in dfs:
            raise ValueError(f"input_key '{kwargs['input_key']}' not found in dataframes {list(dfs)}")

        df = select_columns(dfs[kwargs["input_key"]], kwargs.get("include_columns"))
        trunc = None
        if kwargs.get("is_truncated_needed"):
            trunc = f"TRUNCATE TABLE {kwargs.get('table_to_truncate') or target_name}"
        when = kwargs.get("truncate_before_or_after", "before")

        if trunc and when == "before":
            self.session.sql(trunc).collect()
        strategy = resolve_class(kwargs.get("persistence_strategy", "bulk"))(self.session)
        strategy.persist(df, target_name, mode=kwargs.get("mode", "append"))
        if trunc and when == "after":
            self.session.sql(trunc).collect()
        self.logger.info(f"Persisted to {target_name}", extra={"job_name": job})
        return True
