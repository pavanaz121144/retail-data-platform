from dataprocessorlib.core.utils import as_list
from dataprocessorlib.processors.template_processor import TemplateProcessor


class HashKeyProcessor(TemplateProcessor):
    """SHA-256 of the given columns (null-safe, '|' separated) into hash_key_name."""
    REQUIRED = ("input_keys", "hash_column", "hash_key_name")

    def run(self, selected, p, job):
        from snowflake.snowpark.functions import coalesce, col, concat_ws, lit, sha2
        df = self.single(selected)
        cast = p.get("is_hash_column_datatype_override_required", False)
        parts = [coalesce(col(c).cast("string") if cast else col(c), lit("")) for c in as_list(p["hash_column"])]
        return df.with_column(p["hash_key_name"], sha2(concat_ws(lit("|"), *parts), 256))
