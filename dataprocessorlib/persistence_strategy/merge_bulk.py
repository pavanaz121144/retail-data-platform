from __future__ import annotations

from functools import reduce
from operator import and_


class MergeBulkStrategy:
    """MERGE matched rows of `table` (by key_columns) and apply update_values."""

    def __init__(self, session):
        self.session = session

    def merge(self, table: str, source_df, key_columns, update_values: dict, target_filter: dict | None = None):
        from snowflake.snowpark.functions import lit, when_matched
        target = self.session.table(table)
        cond = reduce(and_, [target[k] == source_df[k] for k in key_columns])
        for col_name, value in (target_filter or {}).items():
            cond = cond & (target[col_name] == lit(value))
        target.merge(source_df, cond, [when_matched().update(update_values)])
