from dataprocessorlib.core.utils import to_col
from dataprocessorlib.processors.template_processor import TemplateProcessor


class ProjectionProcessor(TemplateProcessor):
    """Add derived columns ('expr: ...'), then include/exclude columns."""

    def run(self, selected, p, job):
        df = self.single(selected)
        override = p.get("override_existing_column", False)
        for name, expr in (p.get("additional_columns") or {}).items():
            if name in df.columns and not override:
                continue
            df = df.with_column(name, to_col(expr))
        df = self._select_input_columns(df, p.get("include_columns"), p.get("exclude_columns"))
        return df.distinct() if p.get("distinct") else df
