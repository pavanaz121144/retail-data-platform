# Which class reads which part of config/pipeline.yml

All classes get their settings from the YAML through `PipelineConfig` (config_loader/models.py).
Reach the file from anywhere with `get_config()` (config_loader/provider.py, default `config/pipeline.yml`).

| YAML section | Class that reads it | Keys it uses |
|---|---|---|
| (whole file) | `ConfigLoader` | variables, duplicate keys, full validation (schema.py) |
| (whole file) | `PipelineConfig` / `get_config` | typed access: source(), processor(), target() |
| (whole file) | `Pipeline` | sources, processors, targets, enabled, metadata.pipeline_name |
| `sources[].loader` | `DefaultInputInterface` | include_columns, exclude_columns, filter_expression, cache_data, create_temp_table, temp_table_name |
| | `SequenceInputInterface` | (name only) |
| | `StoredProcedureInterface` | procedure_arguments |
| `sources[].incremental` | `IncrementalConfig` -> `IncrementalManager` | enabled, watermark_column, watermark_expr, watermark_table, watermark_type, filter_to_load_time, source_names, seed_mode, seed_value |
| `processors[]` (all) | `TemplateProcessor` | input_keys, is_separate_output_df_required, output_df_name, job_name |
| `class: latest_raw` | `LatestRawProcessor` | partition_by, latest_column, order_direction |
| `class: projection` | `ProjectionProcessor` | additional_columns, include_columns, exclude_columns, override_existing_column, distinct |
| `class: hash_key` | `HashKeyProcessor` | hash_column, hash_key_name, is_hash_column_datatype_override_required |
| `class: join` | `JoinProcessor` | join_on, join_types, include_columns, exclude_columns, column_mapping, is_distinct |
| `class: union` | `UnionProcessor` | union_all |
| `class: filter` | `FilterProcessor` | filter_expression, include_columns, exclude_columns, column_mapping |
| `class: groupby` | `GroupByProcessor` | group_by_columns, aggregations |
| `class: extract` | `ExtractProcessor` | join_on, join_type, filter_expression, additional_columns, include_columns, exclude_columns, column_mapping |
| `class: cache` | `CacheProcessor` | (input_keys only) |
| `class: cleanup` | `CleanupProcessor` | input_keys |
| `class: batch_id` | `BatchIdGeneratorProcessor` | batch_id_key, job_id_key, batch_id, job_id |
| `class: validate` | `DataValidationProcessor` -> `DataValidator` (checks live in `core/data_validation.py`) | rules (required_columns, not_null, not_blank, unique, row_count, allowed_values, ranges, expressions, keys_not_in_target), on_failure, skip_if_empty |
| `targets[].persistor` | `DefaultOutputInterface` | input_key, include_columns, is_truncated_needed, table_to_truncate, truncate_before_or_after, persistence_strategy, mode |
| | `SCD2TypeOutput` | input_key, key_columns, source_hash_key, target_hash_key, effective_to, current_flag, column_mappings, default_columns, updated_date, updated_by, persistence_strategy, merge_persistence_strategy, use_transaction, delete_detection, delete_flag_column, delete_flag_value, deleted_flag_column, max_delete_ratio, allow_empty_snapshot |
| `*_strategy` | `BulkPersistenceStrategy`, `MergeBulkStrategy` | chosen by name; no keys of their own |
