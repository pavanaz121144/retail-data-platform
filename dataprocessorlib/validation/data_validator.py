"""
Config-driven validator (used by the `validate` processor).
Row-level rules run in ONE aggregation pass; unique needs one extra query per key set.
Shared types and primitives live in core/data_validation.py.
"""
from __future__ import annotations
import logging
from typing import Any, List, Optional, Tuple

from dataprocessorlib.core.data_validation import (  # noqa: F401  (re-exported)
    DataValidationError, RuleResult, check_duplicates, check_keys_not_in_target,
    check_required_columns, column_map,
    enforce_results, null_condition)
from dataprocessorlib.utils.logger import get_json_logger


class DataValidator:
    def __init__(self, df, rules: dict, df_name: str = "dataframe",
                 logger: Optional[logging.Logger] = None, job_name: str = "DataValidation",
                 default_severity: str = "error"):
        self.df, self.rules, self.df_name = df, rules, df_name
        self.logger = logger or get_json_logger("dataprocessorlib.validation")
        self.job, self.sev = job_name, default_severity

    def run(self) -> List[RuleResult]:
        from snowflake.snowpark.functions import col, count, lit, sql_expr, when
        from snowflake.snowpark.functions import sum as sum_

        r, df, sev = self.rules, self.df, self.sev
        cols = column_map(df)
        results: List[RuleResult] = []

        def actual(name: str) -> Optional[str]:
            return cols.get(name.upper())

        def not_found(rule: str, name: str):
            results.append(RuleResult(rule, False, sev, None, f"column '{name}' not found"))

        if r.get("required_columns"):
            results.append(check_required_columns(df, r["required_columns"], sev))

        checks: List[Tuple[str, str, Any]] = []     # (label, severity, "bad row" condition)
        for rule_key, blank in (("not_null", False), ("not_blank", True)):
            for c in r.get(rule_key, []):
                a = actual(c)
                if a is None:
                    not_found(f"{rule_key}[{c}]", c)
                else:
                    checks.append((f"{rule_key}[{c}]", sev, null_condition(a, blank)))
        for c, values in (r.get("allowed_values") or {}).items():
            a = actual(c)
            if a is None:
                not_found(f"allowed_values[{c}]", c)
            else:
                checks.append((f"allowed_values[{c}]", sev, col(a).is_not_null() & ~col(a).in_(values)))
        for c, bounds in (r.get("ranges") or {}).items():
            a = actual(c)
            if a is None:
                not_found(f"ranges[{c}]", c)
                continue
            cond = None
            if "min" in bounds:
                cond = col(a) < lit(bounds["min"])
            if "max" in bounds:
                hi = col(a) > lit(bounds["max"])
                cond = hi if cond is None else (cond | hi)
            checks.append((f"ranges[{c}]", sev, cond))
        for e in r.get("expressions", []):
            checks.append((f"expression[{e['name']}]", e.get("severity", sev), sql_expr(e["violation"])))

        aggs = [count(lit(1)).alias("TOTAL_ROWS")] + [
            sum_(when(cond, lit(1)).otherwise(lit(0))).alias(f"V{i}") for i, (_, _, cond) in enumerate(checks)]
        row = df.agg(*aggs).collect()[0]
        total = row["TOTAL_ROWS"] or 0

        if r.get("row_count"):
            lo, hi = r["row_count"].get("min"), r["row_count"].get("max")
            ok = (lo is None or total >= lo) and (hi is None or total <= hi)
            results.append(RuleResult("row_count", ok, sev, total, f"rows={total}, expected min={lo} max={hi}"))

        for i, (label, s, _) in enumerate(checks):
            bad = row[f"V{i}"] or 0
            results.append(RuleResult(label, bad == 0, s, bad, f"{bad} offending row(s) of {total}"))

        for key in r.get("unique", []):
            results.append(check_duplicates(df, [key] if isinstance(key, str) else list(key), sev))
        for item in r.get("keys_not_in_target", []):
            results.append(check_keys_not_in_target(df, item["table"], item["keys"], item.get("filter"),
                                                    item.get("severity", sev)))
        return results

    def run_and_enforce(self) -> List[RuleResult]:
        return enforce_results(self.run(), self.df_name, self.logger, self.job)
