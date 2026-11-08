"""Pure helpers: parse/format watermark values and apply the incremental window to a df."""
from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

WM = "_WM"  # helper column holding the typed watermark


def parse_value(raw: Any, wm_type: str):
    if raw is None:
        return None
    if wm_type == "timestamp":
        return raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    if wm_type == "date":
        if isinstance(raw, datetime):
            return raw.date()
        return raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])
    return raw if isinstance(raw, Decimal) else Decimal(str(raw))


def format_value(value: Any) -> str:
    return value.isoformat(sep=" ") if isinstance(value, datetime) else str(value)


def typed_watermark(cfg):
    """Column expression of the watermark cast to its declared type."""
    from snowflake.snowpark.functions import col, sql_expr, to_date, to_timestamp
    base = sql_expr(cfg.watermark_expr) if cfg.watermark_expr else col(cfg.watermark_column)
    if cfg.watermark_type == "timestamp":
        return to_timestamp(base)
    if cfg.watermark_type == "date":
        return to_date(base)
    return base.cast("number(38,6)")


def apply_window(df, cfg, lower: Optional[Any], load_time: datetime):
    """Keep rows with  lower < watermark <= load_time  (bounds optional)."""
    from snowflake.snowpark.functions import col, lit
    df = df.with_column(WM, typed_watermark(cfg))
    if lower is not None:
        df = df.filter(col(WM) > lit(lower))
    if cfg.filter_to_load_time and cfg.watermark_type in ("timestamp", "date"):
        upper = load_time.date() if cfg.watermark_type == "date" else load_time
        df = df.filter(col(WM) <= lit(upper))
    return df
