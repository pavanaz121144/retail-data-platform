"""Short aliases -> dotted class paths, plus lazy class resolution."""
from __future__ import annotations
import importlib
from typing import Dict, Optional

P = "dataprocessorlib"
ALIASES: Dict[str, str] = {
    # inputs
    "default": f"{P}.input.default_input_interface.DefaultInputInterface",
    "sequence": f"{P}.input.sequence_input_interface.SequenceInputInterface",
    "stored_procedure": f"{P}.input.stored_procedure_interface.StoredProcedureInterface",
    # processors
    "latest_raw": f"{P}.processors.latest_raw_processor.LatestRawProcessor",
    "projection": f"{P}.processors.projection_processor.ProjectionProcessor",
    "hash_key": f"{P}.processors.hash_key_processor.HashKeyProcessor",
    "join": f"{P}.processors.join_processor.JoinProcessor",
    "union": f"{P}.processors.union_processor.UnionProcessor",
    "filter": f"{P}.processors.filter_processor.FilterProcessor",
    "groupby": f"{P}.processors.groupby_processor.GroupByProcessor",
    "extract": f"{P}.processors.extract_processor.ExtractProcessor",
    "cache": f"{P}.processors.cache_processor.CacheProcessor",
    "cleanup": f"{P}.processors.cleanup_processor.CleanupProcessor",
    "batch_id": f"{P}.processors.batch_id_generator_processor.BatchIdGeneratorProcessor",
    "validate": f"{P}.processors.data_validation_processor.DataValidationProcessor",
    # targets
    "default_output": f"{P}.target.default_output_interface.DefaultOutputInterface",
    "scd2": f"{P}.target.scd2_type_output.SCD2TypeOutput",
    # persistence strategies
    "bulk": f"{P}.persistence_strategy.bulk.BulkPersistenceStrategy",
    "merge_bulk": f"{P}.persistence_strategy.merge_bulk.MergeBulkStrategy",
}


def register(alias: str, dotted_path: str) -> None:
    """Plug in your own component without touching the library."""
    ALIASES[alias] = dotted_path


def resolve_class(ref: str, extra: Optional[Dict[str, str]] = None) -> type:
    ref = {**ALIASES, **(extra or {})}.get(ref, ref)
    module, _, cls = ref.rpartition(".")
    if not module:
        raise ValueError(f"'{ref}' is not a known alias or dotted class path")
    return getattr(importlib.import_module(module), cls)
