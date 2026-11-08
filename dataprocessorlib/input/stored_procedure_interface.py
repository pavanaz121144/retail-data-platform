from dataprocessorlib.core.base import InputInterface
from dataprocessorlib.core.session_mixin import SessionMixin
from dataprocessorlib.core.utils import ExceptionHandler


class StoredProcedureInterface(SessionMixin, InputInterface):
    """Call a stored procedure (source_name); return its result as a DataFrame."""

    @ExceptionHandler.handle_exception
    def load(self, source_name, **kwargs):
        if not source_name:
            raise ValueError("source_name (procedure name) is required")
        result = self.session.call(source_name, *(kwargs.get("procedure_arguments") or []))
        if hasattr(result, "collect"):
            return result
        return self.session.create_dataframe([[result]], schema=["RESULT"])


StoredProcedureLoader = StoredProcedureInterface
