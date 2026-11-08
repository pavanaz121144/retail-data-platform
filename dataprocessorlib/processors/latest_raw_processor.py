from dataprocessorlib.core.utils import as_list
from dataprocessorlib.processors.template_processor import TemplateProcessor


class LatestRawProcessor(TemplateProcessor):
    """Deduplicate: keep the latest row per partition_by, ordered by latest_column."""
    REQUIRED = ("input_keys", "partition_by", "latest_column")

    def run(self, selected, p, job):
        from snowflake.snowpark import Window
        from snowflake.snowpark.functions import col, row_number
        df = self.single(selected)
        cols = as_list(p["latest_column"])
        dirs = as_list(p.get("order_direction")) or ["desc"] * len(cols)
        order = [col(c).desc() if d.lower() == "desc" else col(c).asc() for c, d in zip(cols, dirs)]
        w = Window.partition_by(*as_list(p["partition_by"])).order_by(*order)
        return df.with_column("_RN", row_number().over(w)).filter(col("_RN") == 1).drop("_RN")
