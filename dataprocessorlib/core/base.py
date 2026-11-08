"""
Core abstract interfaces for the Snowpark Data Processing Framework.

This module defines the fundamental abstract base classes that establish the contract
for input loading, data transformation, and output persistence operations within
the framework. All concrete implementations must inherit from these interfaces.
"""

import os
from abc import ABC, abstractmethod
from typing import Any, Optional
from snowflake.snowpark import Session, DataFrame as SnowparkDataFrame
from dataprocessorlib.utils.logger import get_json_logger


class InputInterface(ABC):
    """
    Abstract base class for data loading operations.

    All data loaders should inherit from this class and implement the load method
    to ensure consistent data ingestion patterns across the framework.
    """

    @abstractmethod
    def load(self, source_name: str, **kwargs) -> SnowparkDataFrame:
        """
        Load data from a source into a Snowpark DataFrame.

        Args:
            source_name: Fully qualified name of the source table/view
            **kwargs: Additional parameters for loading (filters, columns, etc.)

        Returns:
            SnowparkDataFrame: Loaded data as a Snowpark DataFrame

        Raises:
            ValueError: If source_name is invalid or not accessible
        """
        pass


class ProcessingInterface(ABC):
    """
    Abstract base class for data transformation operations.

    All data transformers should inherit from this class and implement the transform
    method to ensure consistent transformation patterns and error handling.
    """

    @abstractmethod
    def transform(
        self, dfs: dict[str, SnowparkDataFrame], **kwargs
    ) -> dict[str, SnowparkDataFrame]:
        """
        Transform the input DataFrames according to the processor's logic.

        Args:
            dfs: Dict of input DataFrames keyed by DataFrame identifier
            **kwargs: Additional parameters for transformation

        Returns:
            dict[str, SnowparkDataFrame]: Transformed DataFrames with 'output' key containing result

        Raises:
            ValueError: If required parameters are missing or invalid
        """
        pass


class OutputInterface(ABC):
    """
    Abstract base class for data persistence operations.

    All data persistors should inherit from this class and implement the persist
    method to ensure consistent data write patterns and error handling.
    """

    @abstractmethod
    def persist(
        self, dfs: dict[str, SnowparkDataFrame], target_name: str, **kwargs
    ) -> bool:  # return annotation cut off in photo; docstring says bool
        """
        Persist the DataFrame to a target table.

        Args:
            dfs: Dict of DataFrames to persist
            target_name: Fully qualified name of the target table
            **kwargs: Additional parameters (persistence_strategy, input_key, etc.)

        Returns:
            bool: True if persistence successful, False otherwise

        Raises:
            ValueError: If target_name is invalid or persistence fails
        """
        pass


class BaseProcessor(ProcessingInterface):
    """
    Base processor class providing common functionality for all processors.

    This class implements the Template Method pattern and provides:
    - Standardized logging initialization
    - Common configuration extraction
    - DataFrame selection utilities
    - Output dictionary construction
    - Column mapping and selection logic

    All concrete processors should inherit from this class.
    """

    def __init__(self, session: Optional[Session] = None) -> None:
        """
        Initialize base processor with session and logger.

        Args:
            session: Snowpark session instance (required for table operations)
        """
        self.session = session
        self._logger = None

    @property
    def logger(self) -> Any:
        """Lazy initialization of logger with proper log level."""
        if self._logger is None:
            log_level = os.environ.get('log_level', 'INFO').upper()
            # Rest of this call is cut off in the photo; level= is my guess
            self._logger = get_json_logger(name=self.__class__.__module__, level=log_level)
        return self._logger

    @logger.setter
    def logger(self, value: Any) -> None:
        """Allow setting logger for testing purposes."""
        self._logger = value

    def _get_job_name(self, kwargs: dict[str, Any]) -> str:
        """
        Extract job name from kwargs with default fallback.

        Args:
            kwargs: Configuration parameters

        Returns:
            str: Job name for logging context
        """
        return kwargs.get('job_name', f'{self.__class__.__name__}Job')

    def _validate_required_params(self, params: dict[str, Any], required_keys: list[str]) -> None:
        """
        Validate that all required parameters are present.

        Args:
            params: Configuration parameters to validate
            required_keys: List of required parameter keys

        Raises:
            ValueError: If any required parameter is missing
        """
        missing = [key for key in required_keys if key not in params or params[key] is None]
        if missing:
            raise ValueError(f"{self.__class__.__name__} requires parameters: {missing}")

    def _select_input_columns(
        self,
        df: SnowparkDataFrame,
        include_columns: Optional[list[str]] = None,
        exclude_columns: Optional[list[str]] = None
    ) -> SnowparkDataFrame:
        """
        Apply column selection logic (include/exclude).

        Args:
            df: Input DataFrame
            include_columns: Columns to include (if specified, only these are selected)
            exclude_columns: Columns to exclude (applied after include)

        Returns:
            SnowparkDataFrame: DataFrame with selected columns
        """
        if include_columns:
            df = df.select([c for c in include_columns if c in df.columns])

        if exclude_columns:
            selected_cols = [c for c in df.columns if c not in exclude_columns]
            df = df.select(selected_cols)

        return df

    def _apply_column_mapping(
        self,
        df: SnowparkDataFrame,
        column_mapping: Optional[dict[str, str]],
        job_name: str
    ) -> SnowparkDataFrame:
        """
        Apply column renaming based on mapping configuration.

        Args:
            df: Input DataFrame
            column_mapping: Dict mapping source column to target column name
            job_name: Job name for logging context

        Returns:
            SnowparkDataFrame: DataFrame with renamed columns
        """
        if not column_mapping:
            return df

        self.logger.debug(
            f"Applying column mapping: {column_mapping}",
            extra={"job_name": job_name}
        )

        for src_col, tgt_col in column_mapping.items():
            if src_col in df.columns:
                df = df.with_column_renamed(src_col, tgt_col)
                self.logger.debug(
                    f"Renamed column '{src_col}' to '{tgt_col}'",
                    extra={"job_name": job_name}
                )
            else:
                self.logger.warning(
                    f"Column '{src_col}' not found in DataFrame, skipping rename",
                    extra={"job_name": job_name}
                )

        return df

    def _build_output_dict(
        self,
        original_dfs: dict[str, SnowparkDataFrame],
        result_df: SnowparkDataFrame,
        is_separate_output: bool = False,
        output_name: Optional[str] = None
    ) -> dict[str, SnowparkDataFrame]:
        """
        Construct output dictionary with result DataFrame.

        Args:
            original_dfs: Original input DataFrames
            result_df: Transformed result DataFrame
            is_separate_output: Whether to preserve result in separate key
            output_name: Name for separate output (if is_separate_output is True)

        Returns:
            dict[str, SnowparkDataFrame]: Output dictionary with 'output' and optional named result
        """
        output_dict = dict(original_dfs)
        output_dict['output'] = result_df

        if is_separate_output and output_name:
            output_dict[output_name] = result_df

        return output_dict
