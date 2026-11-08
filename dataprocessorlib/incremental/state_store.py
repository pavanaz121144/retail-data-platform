"""Watermark state table: one row per (pipeline, source)."""
from __future__ import annotations
from typing import Optional


class WatermarkStateStore:
    def __init__(self, session, table: str, pipeline_name: str):
        self.session, self.table, self.pipeline = session, table, pipeline_name

    def ensure_table(self) -> None:
        self.session.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                PIPELINE_NAME   STRING, SOURCE_NAME STRING, WATERMARK_COLUMN STRING,
                WATERMARK_VALUE STRING, WATERMARK_TYPE STRING, ROW_COUNT NUMBER,
                RUN_ID STRING, UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())
        """).collect()

    def get(self, source_name: str) -> Optional[str]:
        rows = self.session.sql(
            f"SELECT WATERMARK_VALUE FROM {self.table} WHERE PIPELINE_NAME = ? AND SOURCE_NAME = ?",
            params=[self.pipeline, source_name]).collect()
        return rows[0]["WATERMARK_VALUE"] if rows else None

    def upsert(self, source_name, column, value, wm_type, row_count, run_id) -> None:
        self.session.sql(f"""
            MERGE INTO {self.table} t
            USING (SELECT ? AS P, ? AS S, ? AS C, ? AS V, ? AS T, ? AS N, ? AS R) s
               ON t.PIPELINE_NAME = s.P AND t.SOURCE_NAME = s.S
            WHEN MATCHED THEN UPDATE SET WATERMARK_COLUMN = s.C, WATERMARK_VALUE = s.V,
                 WATERMARK_TYPE = s.T, ROW_COUNT = s.N, RUN_ID = s.R, UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (PIPELINE_NAME, SOURCE_NAME, WATERMARK_COLUMN,
                 WATERMARK_VALUE, WATERMARK_TYPE, ROW_COUNT, RUN_ID)
                 VALUES (s.P, s.S, s.C, s.V, s.T, s.N, s.R)
        """, params=[self.pipeline, source_name, column, value, wm_type, row_count, run_id]).collect()
