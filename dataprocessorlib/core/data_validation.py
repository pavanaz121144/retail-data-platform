"""
Reusable data-quality checks on Snowpark DataFrames (no pipeline/config needed).

    from dataprocessorlib.core.data_validation import (
        check_nulls, check_duplicates, check_required_columns, check_row_count, validate_dataframe)

    results = [check_required_columns(df, ["ID", "AMT"]),
               *check_nulls(df, ["ID"], include_blank=True),
               check_duplicates(df, ["ID"])]
    enforce_results(results, "orders")            # logs every result, raises DataValidationError on errors

Every check returns a RuleResult(rule, passed, severity, failed_count, detail); nothing raises
until enforce_results() is called, so you can collect several checks first.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from dataprocessorlib.utils.logger import get_json_logger


# --------------------------------------------------------------------- types
@dataclass
class RuleResult:
    rule: str
    passed: bool
    severity: str = "error"            # error | warn
    failed_count: Optional[int] = None
    detail: str = ""


class DataValidationError(Exception):
    def __init__(self, df_name: str, failures: List[RuleResult]):
        self.df_name, self.failures = df_name, failures
        super().__init__(f"Data validation failed for '{df_name}': "
                         + "; ".join(f"{f.rule} ({f.detail})" for f in failures))


# ------------------------------------------------------------------- helpers
def column_map(df) -> Dict[str, str]:
    """UPPERCASE name -> actual column name (quotes stripped for lookup)."""
    return {c.strip('"').upper(): c for c in df.columns}


def missing_columns(df, columns: Sequence[str]) -> List[str]:
    cols = column_map(df)
    return [c for c in columns if c.upper() not in cols]


def null_condition(actual_column: str, include_blank: bool = False):
    """Snowpark condition that is TRUE for NULL (and, optionally, blank/whitespace-only) values."""
    from snowflake.snowpark.functions import col, trim
    c = col(actual_column)
    cond = c.is_null()
    if include_blank:
        cond = cond | (trim(c.cast("string")) == "")
    return cond


def _not_found(rule: str, names: Sequence[str], severity: str) -> RuleResult:
    return RuleResult(rule, False, severity, None, f"column(s) not found: {list(names)}")


# -------------------------------------------------------------------- checks
def check_required_columns(df, required: Sequence[str], severity: str = "error") -> RuleResult:
    miss = missing_columns(df, required)
    return RuleResult("required_columns", not miss, severity, len(miss),
                      f"missing columns: {miss}" if miss else "all present")


def null_counts(df, columns: Sequence[str], include_blank: bool = False) -> Dict[str, int]:
    """NULL (or blank) count per column, computed in ONE pass. Columns must exist."""
    from snowflake.snowpark.functions import lit
    from snowflake.snowpark.functions import sum as sum_
    from snowflake.snowpark.functions import when
    cols = column_map(df)
    actual = [cols[c.upper()] for c in columns]
    aggs = [sum_(when(null_condition(a, include_blank), lit(1)).otherwise(lit(0))).alias(f"N{i}")
            for i, a in enumerate(actual)]
    row = df.agg(*aggs).collect()[0]
    return {c: int(row[f"N{i}"] or 0) for i, c in enumerate(columns)}


def check_nulls(df, columns: Sequence[str], include_blank: bool = False,
                severity: str = "error") -> List[RuleResult]:
    """One RuleResult per column: no NULLs (and no blank strings when include_blank)."""
    label = "not_blank" if include_blank else "not_null"
    miss = missing_columns(df, columns)
    results = [_not_found(f"{label}[{c}]", [c], severity) for c in miss]
    present = [c for c in columns if c not in miss]
    if present:
        total = df.count()
        for c, n in null_counts(df, present, include_blank).items():
            results.append(RuleResult(f"{label}[{c}]", n == 0, severity, n, f"{n} offending row(s) of {total}"))
    return results


def duplicate_count(df, key_columns: Optional[Sequence[str]] = None) -> int:
    """Number of key values that occur more than once (key_columns=None -> whole-row duplicates)."""
    from snowflake.snowpark.functions import col, count, lit
    cols = column_map(df)
    keys = [cols[c.upper()] for c in key_columns] if key_columns else list(df.columns)
    return df.group_by(*keys).agg(count(lit(1)).alias("_C")).filter(col("_C") > 1).count()


def find_duplicates(df, key_columns: Optional[Sequence[str]] = None, limit: Optional[int] = None):
    """DataFrame of duplicated keys with their occurrence count (for inspection / quarantine)."""
    from snowflake.snowpark.functions import col, count, lit
    cols = column_map(df)
    keys = [cols[c.upper()] for c in key_columns] if key_columns else list(df.columns)
    dups = (df.group_by(*keys).agg(count(lit(1)).alias("OCCURRENCES"))
            .filter(col("OCCURRENCES") > 1).sort(col("OCCURRENCES").desc()))
    return dups.limit(limit) if limit else dups


def check_duplicates(df, key_columns: Optional[Sequence[str]] = None, severity: str = "error") -> RuleResult:
    label = f"unique[{','.join(key_columns)}]" if key_columns else "unique[all columns]"
    if key_columns:
        miss = missing_columns(df, key_columns)
        if miss:
            return _not_found(label, miss, severity)
    n = duplicate_count(df, key_columns)
    return RuleResult(label, n == 0, severity, n, f"{n} duplicated key(s)")


def check_keys_not_in_target(df, target_table: str, key_columns: Sequence[str],
                             target_filter: Optional[str] = None, severity: str = "error") -> RuleResult:
    """
    Incremental-load guard for APPEND targets: no key in this batch may already exist in the target
    (catches re-delivered files / a batch loaded twice). Key columns must have the same names in both.
    `target_filter` is SQL limiting which target rows count (e.g. "IS_ACTIVE = TRUE").
    Not meant for SCD2 targets, where existing keys are expected (they become updates).
    """
    from snowflake.snowpark.functions import sql_expr
    label = f"keys_not_in_target[{target_table}]"
    miss = missing_columns(df, key_columns)
    if miss:
        return _not_found(label, miss, severity)
    cols = column_map(df)
    keys = [cols[c.upper()] for c in key_columns]
    target = df.session.table(target_table)
    if target_filter:
        target = target.filter(sql_expr(target_filter))
    n = df.select(*keys).distinct().join(target.select(*keys).distinct(), on=keys, how="inner").count()
    return RuleResult(label, n == 0, severity, n, f"{n} key(s) already present in {target_table}")


def check_row_count(df, min: Optional[int] = None, max: Optional[int] = None,
                    severity: str = "error") -> RuleResult:
    n = df.count()
    ok = (min is None or n >= min) and (max is None or n <= max)
    return RuleResult("row_count", ok, severity, n, f"rows={n}, expected min={min} max={max}")


def check_allowed_values(df, column: str, values: Sequence[Any], severity: str = "error") -> RuleResult:
    from snowflake.snowpark.functions import col
    label = f"allowed_values[{column}]"
    if missing_columns(df, [column]):
        return _not_found(label, [column], severity)
    a = column_map(df)[column.upper()]
    bad = df.filter(col(a).is_not_null() & ~col(a).in_(list(values))).count()
    return RuleResult(label, bad == 0, severity, bad, f"{bad} row(s) outside {list(values)}")


def check_range(df, column: str, min: Any = None, max: Any = None, severity: str = "error") -> RuleResult:
    from snowflake.snowpark.functions import col, lit
    label = f"ranges[{column}]"
    if missing_columns(df, [column]):
        return _not_found(label, [column], severity)
    a = column_map(df)[column.upper()]
    cond = None
    if min is not None:
        cond = col(a) < lit(min)
    if max is not None:
        hi = col(a) > lit(max)
        cond = hi if cond is None else (cond | hi)
    bad = df.filter(cond).count() if cond is not None else 0
    return RuleResult(label, bad == 0, severity, bad, f"{bad} row(s) outside [{min}, {max}]")


def check_expression(df, name: str, violation_sql: str, severity: str = "error") -> RuleResult:
    """`violation_sql` is a SQL condition that is TRUE for bad rows."""
    from snowflake.snowpark.functions import sql_expr
    bad = df.filter(sql_expr(violation_sql)).count()
    return RuleResult(f"expression[{name}]", bad == 0, severity, bad, f"{bad} offending row(s)")


# ------------------------------------------------------------------ enforcing
def enforce_results(results: Sequence[RuleResult], df_name: str = "dataframe",
                    logger: Optional[logging.Logger] = None, job_name: str = "DataValidation") -> List[RuleResult]:
    """Log every result; raise DataValidationError if any error-severity check failed."""
    logger = logger or get_json_logger("dataprocessorlib.validation")
    errors = []
    for r in results:
        extra = {"job_name": job_name, "df_key": df_name, "rule_name": r.rule,
                 "passed": r.passed, "failed_count": r.failed_count}
        if r.passed:
            logger.info(f"validation ok: {r.rule}", extra=extra)
        elif r.severity == "warn":
            logger.warning(f"validation warning: {r.rule} - {r.detail}", extra=extra)
        else:
            logger.error(f"validation FAILED: {r.rule} - {r.detail}", extra=extra)
            errors.append(r)
    if errors:
        raise DataValidationError(df_name, errors)
    return list(results)


def validate_dataframe(df, df_name: str = "dataframe", *, required_columns: Sequence[str] = (),
                       not_null: Sequence[str] = (), not_blank: Sequence[str] = (),
                       unique: Sequence[Any] = (), row_count: Optional[dict] = None,
                       severity: str = "error", logger: Optional[logging.Logger] = None,
                       job_name: str = "DataValidation", enforce: bool = True) -> List[RuleResult]:
    """Run the common checks in one call. `unique` items are a column or a list of columns."""
    results: List[RuleResult] = []
    if required_columns:
        results.append(check_required_columns(df, required_columns, severity))
    if not_null:
        results += check_nulls(df, not_null, False, severity)
    if not_blank:
        results += check_nulls(df, not_blank, True, severity)
    for key in unique:
        results.append(check_duplicates(df, [key] if isinstance(key, str) else list(key), severity))
    if row_count:
        results.append(check_row_count(df, row_count.get("min"), row_count.get("max"), severity))
    return enforce_results(results, df_name, logger, job_name) if enforce else results
