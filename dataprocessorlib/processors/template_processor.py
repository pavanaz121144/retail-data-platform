"""
Template-method base for single-output processors.

transform() is fixed:  validate params -> pick input df(s) -> run() -> log sample
-> build output dict ('output' + optional named output).  Subclasses implement run() only.
"""
from abc import abstractmethod

from dataprocessorlib.core.base import BaseProcessor
from dataprocessorlib.core.utils import ExceptionHandler, log_sample_rows, pick_dataframe_to_process


class TemplateProcessor(BaseProcessor):
    REQUIRED: tuple = ("input_keys",)   # params that must be present
    NEEDS_SINGLE_DF: bool = True        # False for join / union

    @ExceptionHandler.handle_exception
    def transform(self, dfs, **kwargs):
        job = self._get_job_name(kwargs)
        name = self.__class__.__name__
        self._validate_required_params(kwargs, list(self.REQUIRED))
        selected = pick_dataframe_to_process(dfs, kwargs.get("input_keys"), name, self.NEEDS_SINGLE_DF)
        self.logger.info(f"{name} started", extra={"job_name": job, "inputs": list(selected)})

        result = self.run(selected, kwargs, job)

        log_sample_rows(self.logger, result, job, f"{name} output sample:")
        out = self._build_output_dict(dfs, result,
                                      kwargs.get("is_separate_output_df_required", False),
                                      kwargs.get("output_df_name"))
        self.logger.info(f"{name} completed", extra={"job_name": job, "output_df": kwargs.get("output_df_name")})
        return out

    @abstractmethod
    def run(self, selected: dict, params: dict, job_name: str):
        """Return the result DataFrame."""

    @staticmethod
    def single(selected: dict):
        return next(iter(selected.values()))
