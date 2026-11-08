from dataprocessorlib.core.utils import apply_filter, as_list, to_col
from dataprocessorlib.processors.template_processor import TemplateProcessor


class ExtractProcessor(TemplateProcessor):
    """Extract a slice: optional join, filter, derived columns, select, rename."""
    NEEDS_SINGLE_DF = False

    def run(self, selected, p, job):
        frames = list(selected.values())
        df = frames[0]
        for other in frames[1:]:
            df = df.join(other, on=as_list(p.get("join_on")), how=p.get("join_type", "inner"))
        df = apply_filter(df, p.get("filter_expression"))
        for name, expr in (p.get("additional_columns") or {}).items():
            df = df.with_column(name, to_col(expr))
        df = self._select_input_columns(df, p.get("include_columns"), p.get("exclude_columns"))
        return self._apply_column_mapping(df, p.get("column_mapping"), job)
