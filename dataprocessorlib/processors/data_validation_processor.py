from dataprocessorlib.core.base import BaseProcessor
from dataprocessorlib.core.utils import ExceptionHandler, pick_dataframe_to_process
from dataprocessorlib.validation import DataValidator


class DataValidationProcessor(BaseProcessor):
    """
    Pass-through step that validates each input df against `rules`
    (see validation/rules.py). on_failure: fail (default) | warn.
    skip_if_empty: true skips a df with zero rows - use it after an incremental source,
    where "no new data" is a normal outcome and shouldn't trip row_count / required rules.
    """

    @ExceptionHandler.handle_exception
    def transform(self, dfs, **kwargs):
        job = self._get_job_name(kwargs)
        self._validate_required_params(kwargs, ["input_keys", "rules"])
        severity = "warn" if kwargs.get("on_failure", "fail") == "warn" else "error"
        selected = pick_dataframe_to_process(dfs, kwargs["input_keys"], "DataValidationProcessor")
        for key, df in selected.items():
            if kwargs.get("skip_if_empty") and df.count() == 0:
                self.logger.info(f"'{key}' is empty (no new incremental rows) - validation skipped",
                                 extra={"job_name": job, "df_key": key})
                continue
            DataValidator(df, kwargs["rules"], df_name=key, logger=self.logger,
                          job_name=job, default_severity=severity).run_and_enforce()
        return dict(dfs)
