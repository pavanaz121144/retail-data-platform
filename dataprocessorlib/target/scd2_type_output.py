from dataprocessorlib.core.base import OutputInterface
from dataprocessorlib.core.registry import resolve_class
from dataprocessorlib.core.session_mixin import SessionMixin
from dataprocessorlib.core.utils import ExceptionHandler, apply_column_mapping, to_col

REQUIRED = ["input_key", "key_columns", "source_hash_key", "target_hash_key",
            "effective_to", "current_flag"]
DELETE_MODES = ("none", "full_snapshot", "source_flag")


class SCD2TypeOutput(SessionMixin, OutputInterface):
    """
    SCD Type 2 with optional soft delete, all applied in one transaction.

      changed rows   = no active target hash, or hash differs      -> close old version, insert new
      soft deletes   = delete_detection:
          none           (default) nothing is ever deleted
          source_flag    source rows where <delete_flag_column> = <delete_flag_value> close the active
                         version and are NOT inserted. Safe with incremental sources.
          full_snapshot  the batch is the COMPLETE source: active target keys missing from it are
                         closed. NEVER use with incremental sources (guarded by max_delete_ratio and
                         allow_empty_snapshot, and rejected by config validation).
      closing        = IS_ACTIVE false, effective_to/updated_date = now, updated_by = user,
                       plus <deleted_flag_column> = TRUE for deletes (optional).

    Params: use_transaction (default true), max_delete_ratio (default 0.5), allow_empty_snapshot (false).
    """

    @ExceptionHandler.handle_exception
    def persist(self, dfs, target_name, **kwargs) -> bool:
        from snowflake.snowpark.functions import col, current_timestamp, lit

        job = self._job_name(kwargs)
        self._validate_required_params(kwargs, REQUIRED)
        if kwargs["input_key"] not in dfs:
            raise ValueError(f"input_key '{kwargs['input_key']}' not found in dataframes {list(dfs)}")
        mode = kwargs.get("delete_detection", "none")
        if mode not in DELETE_MODES:
            raise ValueError(f"delete_detection must be one of {DELETE_MODES}, got {mode!r}")
        if mode == "source_flag":
            self._validate_required_params(kwargs, ["delete_flag_column"])

        df, defaults = dfs[kwargs["input_key"]], kwargs.get("default_columns") or {}
        keys, src_h, tgt_h = kwargs["key_columns"], kwargs["source_hash_key"], kwargs["target_hash_key"]
        flag, eff_to = kwargs["current_flag"], kwargs["effective_to"]

        # ---- 1. split the batch / find deletes -------------------------------------------
        live, deleted_keys = df, None
        if mode == "source_flag":
            dcol = kwargs["delete_flag_column"]
            is_del = col(dcol).is_not_null() & (col(dcol) == lit(kwargs.get("delete_flag_value", True)))
            deleted_keys = df.filter(is_del).select(*keys)
            live = df.filter(~is_del).drop(dcol)
        elif mode == "full_snapshot":
            deleted_keys = self._missing_from_source(target_name, live, keys, flag, kwargs)

        # ---- 2. materialise EVERYTHING before touching the target ------------------------
        # (the target changes below, so diffs must not be recomputed lazily; and cache_result is DDL,
        #  which must not run inside the transaction)
        changed = live.filter(col(tgt_h).is_null() | (col(tgt_h) != col(src_h))).cache_result()
        n_changed = changed.count()
        n_deleted = 0
        if deleted_keys is not None:
            deleted_keys = deleted_keys.cache_result()
            n_deleted = deleted_keys.count()
        self.logger.info(f"SCD2 changed={n_changed} deleted={n_deleted} mode={mode}",
                         extra={"job_name": job, "changed_rows": n_changed, "deleted_rows": n_deleted})
        if n_changed == 0 and n_deleted == 0:
            return True

        close = {flag: lit(False), eff_to: current_timestamp()}
        if kwargs.get("updated_date"):
            close[kwargs["updated_date"]] = current_timestamp()
        if kwargs.get("updated_by"):
            close[kwargs["updated_by"]] = to_col(defaults.get(kwargs["updated_by"], "expr: CURRENT_USER"))

        keys_df = new = None
        if n_changed:
            keys_df = changed.select(*keys).cache_result()
            new = apply_column_mapping(changed.drop(src_h, tgt_h), kwargs.get("column_mappings") or {})
            for c, expr in defaults.items():
                if c not in new.columns:
                    new = new.with_column(c, to_col(expr))
            new = new.with_column(flag, lit(True)).cache_result()

        merge = resolve_class(kwargs.get("merge_persistence_strategy", "merge_bulk"))(self.session)
        bulk = resolve_class(kwargs.get("persistence_strategy", "bulk"))(self.session)

        # ---- 3. apply: close old versions, soft-delete, insert new versions --------------
        def apply():
            if n_changed:
                merge.merge(target_name, keys_df, keys, close, target_filter={flag: True})
            if n_deleted:
                del_close = dict(close)
                if kwargs.get("deleted_flag_column"):
                    del_close[kwargs["deleted_flag_column"]] = lit(True)
                merge.merge(target_name, deleted_keys, keys, del_close, target_filter={flag: True})
            if n_changed:
                bulk.insert(new, target_name)

        if kwargs.get("use_transaction", True):
            with self.transaction():
                apply()
        else:
            apply()
        return True

    def _missing_from_source(self, target_name, live, keys, flag, kwargs):
        """full_snapshot: active target keys absent from the batch (with safety guards)."""
        from snowflake.snowpark.functions import col, lit
        if live.count() == 0 and not kwargs.get("allow_empty_snapshot", False):
            raise ValueError("full_snapshot with an empty source would close every active row; "
                             "set allow_empty_snapshot: true if that is really intended")
        active = self.session.table(target_name).filter(col(flag) == lit(True)).select(*keys).distinct()
        missing = active.join(live.select(*keys).distinct(), on=keys, how="anti")
        total, n = active.count(), missing.count()
        ratio = kwargs.get("max_delete_ratio", 0.5)
        if total and n > ratio * total:
            raise ValueError(f"full_snapshot would close {n} of {total} active rows (> max_delete_ratio "
                             f"{ratio}); refusing - is the source snapshot complete?")
        return missing
