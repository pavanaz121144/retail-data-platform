from dataprocessorlib.core.utils import apply_filter
from dataprocessorlib.processors.template_processor import TemplateProcessor


class FilterProcessor(TemplateProcessor):
    """filter_expression + include/exclude + column_mapping."""
    REQUIRED = ("input_keys", "filter_expression")

    def run(self, selected, p, job):
        df = apply_filter(self.single(selected), p["filter_expression"])
        df = self._select_input_columns(df, p.get("include_columns"), p.get("exclude_columns"))
        return self._apply_column_mapping(df, p.get("column_mapping"), job)
