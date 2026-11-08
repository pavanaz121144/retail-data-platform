from dataprocessorlib.core.base import InputInterface
from dataprocessorlib.core.session_mixin import SessionMixin
from dataprocessorlib.core.utils import ExceptionHandler


class SequenceInputInterface(SessionMixin, InputInterface):
    """One-row DataFrame holding NEXTVAL of the named sequence."""

    @ExceptionHandler.handle_exception
    def load(self, source_name, **kwargs):
        if not source_name:
            raise ValueError("source_name (sequence name) is required")
        return self.session.sql(f"SELECT {source_name}.NEXTVAL AS NEXT_VAL")
