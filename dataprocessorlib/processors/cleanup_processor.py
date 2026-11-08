from dataprocessorlib.core.base import BaseProcessor
from dataprocessorlib.core.utils import ExceptionHandler, as_list


class CleanupProcessor(BaseProcessor):
    """Drop intermediate DataFrames from the shared dict to free memory."""

    @ExceptionHandler.handle_exception
    def transform(self, dfs, **kwargs):
        job = self._get_job_name(kwargs)
        self._validate_required_params(kwargs, ["input_keys"])
        out = dict(dfs)
        dropped = [k for k in as_list(kwargs["input_keys"]) if out.pop(k, None) is not None]
        self.logger.info("Cleanup completed", extra={"job_name": job, "dropped": dropped})
        return out
