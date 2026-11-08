from dataprocessorlib.core.utils import as_list
from dataprocessorlib.processors.template_processor import TemplateProcessor


class GroupByProcessor(TemplateProcessor):
    """group_by_columns + aggregations {column: func}; output column = <COLUMN>_<FUNC>."""
    REQUIRED = ("input_keys", "group_by_columns", "aggregations")

    def run(self, selected, p, job):
        from snowflake.snowpark import functions as F
        aggs = [getattr(F, fn)(F.col(c)).alias(f"{c}_{fn}".upper()) for c, fn in p["aggregations"].items()]
        return self.single(selected).group_by(*as_list(p["group_by_columns"])).agg(*aggs)
