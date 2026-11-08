"""Validated view of a source's `incremental:` YAML block."""
from __future__ import annotations
from dataclasses import dataclass, field, fields
from typing import List, Optional

VALID_TYPES = {"timestamp", "date", "numeric"}
VALID_SEED = {"full", "filter", "fail"}


@dataclass
class IncrementalConfig:
    enabled: bool = False
    watermark_column: Optional[str] = None   # column that moves forward (e.g. FILE_AS_OF_DATE)
    watermark_expr: Optional[str] = None     # optional SQL; overrides watermark_column
    watermark_table: Optional[str] = None    # state table (schema.table)
    watermark_type: str = "timestamp"        # timestamp | date | numeric
    filter_to_load_time: bool = True         # ignore rows newer than this run's start
    source_names: List[str] = field(default_factory=list)  # state keys; empty -> source name
    seed_mode: str = "filter"                # first run: full | filter | fail
    seed_value: Optional[str] = None         # lower bound used when seed_mode == filter

    @classmethod
    def from_dict(cls, raw: Optional[dict]) -> "IncrementalConfig":
        raw = raw or {}
        unknown = set(raw) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"Unknown incremental option(s): {sorted(unknown)}")
        cfg = cls(**raw)
        cfg.source_names = list(cfg.source_names or [])
        if cfg.enabled:
            if not cfg.watermark_table:
                raise ValueError("incremental.watermark_table is required")
            if not (cfg.watermark_column or cfg.watermark_expr):
                raise ValueError("incremental needs watermark_column or watermark_expr")
            if cfg.watermark_type not in VALID_TYPES:
                raise ValueError(f"watermark_type must be one of {sorted(VALID_TYPES)}")
            if cfg.seed_mode not in VALID_SEED:
                raise ValueError(f"seed_mode must be one of {sorted(VALID_SEED)}")
        return cfg
