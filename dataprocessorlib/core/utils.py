"""
Utility functions for the data processor library.
"""

import logging
import traceback
import functools
from collections.abc import Callable
from typing import Any, Optional, Union

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException


def apply_filter(df: SnowparkDataFrame, filter_expression: Optional[str]) -> SnowparkDataFrame:
    """
    Utility function to apply a filter expression to a Snowpark DataFrame.
    Args:
        df: Snowpark DataFrame
        filter_expression: str, filter expression to apply
    Returns:
        Filtered Snowpark DataFrame
    """
    if filter_expression:
        if "col(" in filter_expression:
            from snowflake.snowpark.functions import col
            df = df.filter(eval(filter_expression, {'col': col}))
        else:
            df = df.filter(filter_expression)
    return df


def log_sample_rows(logger: logging.Logger, df: SnowparkDataFrame, job_name: str,
                    context_msg: str = "Sample records:", limit: int = 5) -> None:
    """
    Utility to log the top N rows and count of a DataFrame for debugging.

    Args:
        logger: Logger instance for output
        df: Snowpark DataFrame to sample
        job_name: Name of the job for logging context
        context_msg: Message to log with samples (default: "Sample records:")
        limit: Number of rows to display (default: 5)

    Fetches up to ``limit`` rows using ``df.limit(limit).collect()`` instead of ``df.show()``.  # (line cut off in photo)
    Handles exceptions internally.
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return

    logger.debug(f"{context_msg}", extra={"job_name": job_name})

    cols = None
    try:
        cols = df.columns
    except Exception as e:
        logger.debug(f"Could not fetch column metadata: {e}", extra={"job_name": job_name})

    rows = None
    try:
        rows = df.limit(limit).collect()
    except Exception as e:
        logger.debug(f"Could not fetch sample rows: {e}", extra={"job_name": job_name})

    if rows and cols:
        for r in rows:
            row_dict = dict(zip(cols, list(r)))
            logger.debug(f"{row_dict}", extra={"job_name": job_name})
    elif rows:
        # Columns are unavailable; log raw row data instead.
        for r in rows:
            logger.debug(f"{list(r)}", extra={"job_name": job_name})
    else:
        logger.debug("No rows returned for sampling.", extra={"job_name": job_name})

    try:
        row_count = df.count()
        logger.debug(f"Total record count: {row_count}", extra={"job_name": job_name})
        columns = df.columns
        logger.debug(f"Columns in extracted DataFrame: {columns}", extra={"job_name": job_name})
    except Exception as e:
        traceback.print_exc()
        msg = f"Could not fetch sample records or count for debug: {e}"
        logger.debug(msg, extra={"job_name": job_name})


def pick_dataframe_to_process(dfs: dict[str, SnowparkDataFrame], input_keys: Optional[list[str]],
                              context: str, needs_single_df: bool = False) -> dict[str, SnowparkDataFrame]:
    """
    Select DataFrames from a dictionary based on input keys or default logic.

    Args:
        dfs: dictionary of DataFrames
        input_keys: list of keys to select specific DataFrames
        context: Context string for error messages
        needs_single_df: Whether only a single DataFrame is expected

    Returns:
        dictionary of selected DataFrames

    Raises:
        ValueError: If no valid DataFrames can be selected
    """
    if input_keys:
        if not isinstance(input_keys, list):
            raise ValueError("input_keys must be a list of DataFrame keys.")
        selected_dfs = {k: dfs[k] for k in input_keys if k in dfs}
        if not selected_dfs:
            raise ValueError(f"{context}: None of the specified 'input_keys' found in input dict.")
    elif 'output' in dfs:
        selected_dfs = {'output': dfs['output']}
    elif isinstance(dfs, dict) and len(dfs) == 1 and needs_single_df:
        selected_dfs = dict(dfs)
    elif isinstance(dfs, dict) and len(dfs):
        selected_dfs = dfs
    else:
        err_msg = (
            f"{context}: Unable to determine which DataFrame(s) to process. Please specify "  # cut off in photo
            f"ensure 'output' is present, or provide only one DataFrame in input dict."
        )
        raise ValueError(err_msg)
    return selected_dfs


def apply_column_mapping(input_df: SnowparkDataFrame, column_mapping: dict[str, str]) -> SnowparkDataFrame:
    """
    Apply column renaming mappings to a DataFrame.

    Args:
        input_df: Input Snowpark DataFrame
        column_mapping: dictionary mapping old column names to new names

    Returns:
        DataFrame with renamed columns
    """
    logging.info(f"Applying column mapping: {column_mapping}")
    for src_col, tgt_col in column_mapping.items():
        if src_col in input_df.columns and tgt_col not in input_df.columns:
            input_df = input_df.with_column_renamed(src_col, tgt_col)
            logging.info(f"Renamed column {src_col} to {tgt_col}")
        elif src_col in input_df.columns and tgt_col in input_df.columns:
            msg = f"Target column {tgt_col} already exists in DataFrame. Skipping rename for {src_col}."  # end cut off in photo
            logging.debug(msg)
        else:
            msg = f"Source column {src_col} not found in DataFrame. Skipping rename."
            logging.debug(msg)
    return input_df


class ExceptionHandler(object):
    """
    Utility class providing exception handling decorators for method calls.
    """

    @staticmethod
    def handle_exception(
        method: Callable[..., Union[dict[str, SnowparkDataFrame], SnowparkDataFrame]],
    ) -> Callable[..., Union[dict[str, SnowparkDataFrame], SnowparkDataFrame]]:
        """
        Decorator to handle exceptions in method calls with logging.

        Args:
            method: Method to wrap with exception handling

        Returns:
            Wrapped method with exception handling and logging
        """

        @functools.wraps(method)
        def wrapper(self: "ExceptionHandler", *args: object,
                    **kwargs: object) -> Union[dict[str, SnowparkDataFrame], SnowparkDataFrame]:
            """
            Wrapper function for logging method entry, exit, and exceptions.

            Logs the entry and exit of the decorated method, including keyword arguments.
            Catches and logs exceptions, specifically handling SnowparkSQLException separately.
            Logs exception details and stack trace for general exceptions.

            Args:
                *args: Variable length argument list for the method.
                **kwargs: Arbitrary keyword arguments for the method.

            Returns:
                The result of the wrapped method.

            Raises:
                SnowparkSQLException: If a Snowpark SQL exception occurs.
                Exception: For any other exceptions raised by the method.
            """
            logger = getattr(self, 'logger', None)
            try:
                result = method(self, *args, **kwargs)
            except SnowparkSQLException as spe:
                if logger:
                    logger.error(f"SnowparkSQLException in {method.__name__}: {spe}")
                raise
            except Exception as e:
                if logger:
                    logger.error(f"Exception in {method.__name__}: {e}")
                    logger.debug(traceback.format_exc())
                raise
            return result

        return wrapper


# ---------------------------------------------------------------------------
# Framework helpers (added on top of the original utilities)
# ---------------------------------------------------------------------------

def as_list(value: Any) -> list:
    """None -> [], 'a' -> ['a'], iterable -> list."""
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def to_col(value: Any):
    """'expr: <SQL>' -> SQL expression column; anything else -> literal column."""
    from snowflake.snowpark.functions import lit, sql_expr
    if isinstance(value, str) and value.strip().lower().startswith("expr:"):
        return sql_expr(value.strip()[5:].strip())
    return lit(value)


def select_columns(df: SnowparkDataFrame, include: Optional[list] = None,
                   exclude: Optional[list] = None) -> SnowparkDataFrame:
    """include/exclude column selection for components that are not processors."""
    include, exclude = as_list(include), as_list(exclude)
    if include:
        df = df.select([c for c in include if c in df.columns])
    if exclude:
        df = df.select([c for c in df.columns if c not in exclude])
    return df
