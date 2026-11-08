from dataprocessorlib.core.base import InputInterface
from dataprocessorlib.core.session_mixin import SessionMixin
from dataprocessorlib.core.utils import ExceptionHandler, apply_filter, log_sample_rows, select_columns


class DefaultInputInterface(SessionMixin, InputInterface):
    """Load a table/view/stream with optional filter, column pruning and caching."""

    @ExceptionHandler.handle_exception
    def load(self, source_name, **kwargs):
        job = self._job_name(kwargs)
        if not source_name:
            raise ValueError("source_name is required")
        self.logger.info(f"Loading {source_name}", extra={"job_name": job})

        # consume_stream: source_name is a stream; its offset advances when a DML reads it.
        df = self.session.table(source_name)
        df = apply_filter(df, kwargs.get("filter_expression"))
        df = select_columns(df, kwargs.get("include_columns"), kwargs.get("exclude_columns"))

        if kwargs.get("create_temp_table"):
            temp = kwargs.get("temp_table_name")
            if not temp:
                raise ValueError("temp_table_name required when create_temp_table is true")
            df.write.save_as_table(temp, mode="overwrite", table_type="temporary")
            df = self.session.table(temp)
        if kwargs.get("cache_data"):
            df = df.cache_result()

        log_sample_rows(self.logger, df, job, f"Loaded {source_name}:")
        return df


DefaultLoader = DefaultInputInterface
