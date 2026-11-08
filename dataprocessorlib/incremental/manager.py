"""
Incremental logic, in two phases (so a failed run never advances the watermark):

  prepare(source_df, cfg, source_name)   -> filtered df       (at load time)
      1. read last watermark(s) from the state table (min across source_names)
      2. no state yet -> seed_mode: full | filter (seed_value) | fail
      3. keep rows  last < watermark <= load_time
      4. cache the slice, record max(watermark) + row count as *pending*
  commit()                                                     (after targets succeed)
      upsert pending watermarks into the state table
"""
from __future__ import annotations
from dataprocessorlib.utils.logger import get_json_logger
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

from dataprocessorlib.incremental.config import IncrementalConfig
from dataprocessorlib.incremental.state_store import WatermarkStateStore
from dataprocessorlib.incremental.windows import WM, apply_window, format_value, parse_value

log = get_json_logger("dataprocessorlib.incremental")


@dataclass
class _Pending:
    store: WatermarkStateStore
    keys: List[str]
    column: str
    wm_type: str
    value: Any
    row_count: int


class IncrementalManager:
    def __init__(self, session, pipeline_name: str, run_id: Optional[str] = None,
                 load_time: Optional[datetime] = None):
        self.session, self.pipeline = session, pipeline_name
        self.run_id = run_id or str(uuid.uuid4())
        self.load_time = load_time or datetime.utcnow()   # same timezone as the watermark column
        self._pending: List[_Pending] = []

    def prepare(self, df, raw_cfg: Optional[dict], source_name: str):
        cfg = raw_cfg if isinstance(raw_cfg, IncrementalConfig) else IncrementalConfig.from_dict(raw_cfg)
        if not cfg.enabled:
            return df
        from snowflake.snowpark.functions import count, lit, max as max_

        store = WatermarkStateStore(self.session, cfg.watermark_table, self.pipeline)
        store.ensure_table()
        keys = cfg.source_names or [source_name]
        lower = self._lower_bound(store, keys, cfg)

        sliced = apply_window(df, cfg, lower, self.load_time).cache_result()
        new_wm, rows = sliced.agg(max_(WM), count(lit(1))).collect()[0]
        log.info("incremental[%s]: lower=%s upper=%s rows=%s new_watermark=%s",
                 source_name, lower, self.load_time if cfg.filter_to_load_time else None, rows, new_wm)

        if new_wm is not None:    # no rows -> keep the old watermark
            self._pending.append(_Pending(store, keys, cfg.watermark_column or "<expr>",
                                          cfg.watermark_type, parse_value(new_wm, cfg.watermark_type), rows))
        return sliced.drop(WM)

    def commit(self) -> None:
        for p in self._pending:
            for key in p.keys:
                p.store.upsert(key, p.column, format_value(p.value), p.wm_type, p.row_count, self.run_id)
        self._pending = []

    def discard(self) -> None:
        self._pending = []

    # ------------------------------------------------------------------
    def _lower_bound(self, store, keys, cfg):
        stored = [store.get(k) for k in keys]
        if all(v is not None for v in stored):
            return min(parse_value(v, cfg.watermark_type) for v in stored)   # most conservative
        if cfg.seed_mode == "fail":
            raise RuntimeError(f"No watermark for {keys} and seed_mode=fail")
        if cfg.seed_mode == "filter" and cfg.seed_value:
            return parse_value(cfg.seed_value, cfg.watermark_type)
        return None   # full first load
