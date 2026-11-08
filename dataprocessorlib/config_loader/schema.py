"""Static validation of a loaded pipeline config. Returns a list of error strings."""
from __future__ import annotations
import importlib.util
import re
from typing import Any, Dict, List, Optional

from dataprocessorlib.incremental.config import IncrementalConfig
from dataprocessorlib.validation.rules import validate_rules_schema

ALLOWED_TOP = {"metadata", "sources", "processors", "targets"}

PROCESSOR_REQUIRED = {
    "latest_raw": ["input_keys", "partition_by", "latest_column"],
    "projection": ["input_keys"],
    "hash_key": ["input_keys", "hash_column", "hash_key_name"],
    "join": ["input_keys", "join_on"],
    "union": ["input_keys"],
    "filter": ["input_keys", "filter_expression"],
    "groupby": ["input_keys", "group_by_columns", "aggregations"],
    "extract": ["input_keys"],
    "cache": ["input_keys"],
    "cleanup": ["input_keys"],
    "batch_id": ["input_keys", "batch_id_key", "job_id_key"],
    "validate": ["input_keys", "rules"],
}
MIN_INPUTS = {"join": 2, "union": 2}
TARGET_REQUIRED = {
    "scd2": ["input_key", "key_columns", "source_hash_key", "target_hash_key", "effective_to", "current_flag"],
    "default_output": ["input_key"],
}
BOOL_KEYS = {"is_separate_output_df_required", "is_distinct", "distinct", "union_all",
             "override_existing_column", "is_hash_column_datatype_override_required",
             "cache_data", "skip_if_empty", "use_transaction", "allow_empty_snapshot", "create_temp_table", "consume_stream", "is_truncated_needed"}

_UNSAFE_FILTER = re.compile(r"__|\bimport\b|\bexec\b|\beval\b|\bopen\b|\bos\b|\bsys\b|\bgetattr\b|\blambda\b|\bcompile\b")


def _resolvable(ref: Any, aliases: Dict[str, str]) -> bool:
    if not isinstance(ref, str):
        return False
    module, _, cls = aliases.get(ref, ref).rpartition(".")
    if not module or not cls:
        return False
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def _canonical(ref: str, aliases: Dict[str, str]) -> Optional[str]:
    if ref in aliases:
        return ref
    for alias, path in aliases.items():
        if path == ref:
            return alias
    return None


def _check_filter(expr: Any, where: str, errs: List[str]) -> None:
    if expr is None:
        return
    if not isinstance(expr, str) or not expr.strip():
        errs.append(f"{where}: filter_expression must be a non-empty string")
    elif "col(" in expr and (_UNSAFE_FILTER.search(expr) or expr.count("(") != expr.count(")")):
        # utils.apply_filter eval()s expressions containing "col(" -> screen them here
        errs.append(f"{where}: filter_expression contains unsafe or unbalanced code: {expr!r}")


def _check_bools(param: dict, where: str, errs: List[str]) -> None:
    for k in BOOL_KEYS & set(param):
        if not isinstance(param[k], bool):
            errs.append(f"{where}.{k}: must be true/false, got {param[k]!r}")


def validate_config(cfg: Any, aliases: Dict[str, str]) -> List[str]:
    errs: List[str] = []
    if not isinstance(cfg, dict):
        return ["root: config must be a mapping"]
    for k in set(cfg) - ALLOWED_TOP:
        errs.append(f"root.{k}: unknown top-level key (allowed: {sorted(ALLOWED_TOP)})")
    structural: List[str] = []
    for section in ("sources", "processors", "targets"):
        if not isinstance(cfg.get(section, []), list):
            structural.append(f"{section}: must be a list")
    for section in ("sources", "targets"):
        if not cfg.get(section):
            structural.append(f"{section}: at least one entry is required")
    if structural:          # cannot walk the sections safely
        return errs + structural

    available: set = set()
    has_output = False

    # ---------------- sources
    for i, s in enumerate(cfg["sources"]):
        w = f"sources[{i}]"
        if not isinstance(s, dict):
            errs.append(f"{w}: must be a mapping"); continue
        if not isinstance(s.get("name"), str) or not s["name"].strip():
            errs.append(f"{w}.name: required string")
        if not _resolvable(s.get("loader", "default"), aliases):
            errs.append(f"{w}.loader: cannot resolve {s.get('loader', 'default')!r}")
        param = s.get("param") or {}
        if not isinstance(param, dict):
            errs.append(f"{w}.param: must be a mapping"); param = {}
        out = param.get("output_df_name")
        if not isinstance(out, str) or not out:
            errs.append(f"{w}.param.output_df_name: required string")
        elif out in available:
            errs.append(f"{w}.param.output_df_name: duplicate dataframe name {out!r}")
        else:
            available.add(out)
        for lst in ("include_columns", "exclude_columns"):
            if lst in param and not isinstance(param[lst], list):
                errs.append(f"{w}.param.{lst}: must be a list")
        _check_filter(param.get("filter_expression"), f"{w}.param", errs)
        _check_bools(param, f"{w}.param", errs)
        if param.get("create_temp_table") and not param.get("temp_table_name"):
            errs.append(f"{w}.param.temp_table_name: required when create_temp_table is true")
        try:
            IncrementalConfig.from_dict(s.get("incremental"))
        except (ValueError, TypeError) as e:
            errs.append(f"{w}.incremental: {e}")

    # ---------------- processors (in execution order)
    seen_ids: set = set()
    for i, p in enumerate(cfg.get("processors") or []):
        w = f"processors[{i}]" + (f" ({p.get('id')})" if isinstance(p, dict) and p.get("id") else "")
        if not isinstance(p, dict):
            errs.append(f"{w}: must be a mapping"); continue
        if p.get("id"):
            if p["id"] in seen_ids:
                errs.append(f"{w}.id: duplicate id {p['id']!r}")
            seen_ids.add(p["id"])
        ref = p.get("class")
        if not _resolvable(ref, aliases):
            errs.append(f"{w}.class: cannot resolve {ref!r}"); continue
        kind = _canonical(ref, aliases)
        param = p.get("param") or {}
        if not isinstance(param, dict):
            errs.append(f"{w}.param: must be a mapping"); continue

        missing = [k for k in PROCESSOR_REQUIRED.get(kind, []) if param.get(k) in (None, "", [], {})]
        if missing:
            errs.append(f"{w}.param: missing required {missing}")
        keys = param.get("input_keys")
        if keys is not None:
            if not (isinstance(keys, list) and keys and all(isinstance(k, str) for k in keys)):
                errs.append(f"{w}.param.input_keys: must be a non-empty list of names")
            else:
                if len(keys) < MIN_INPUTS.get(kind, 1):
                    errs.append(f"{w}.param.input_keys: {kind} needs at least {MIN_INPUTS[kind]} inputs")
                for k in keys:
                    if k not in available and not (k == "output" and has_output):
                        errs.append(f"{w}.param.input_keys: {k!r} is not produced by any earlier step")
        _check_filter(param.get("filter_expression"), f"{w}.param", errs)
        _check_bools(param, f"{w}.param", errs)
        if kind == "validate" and "rules" in param:
            errs += validate_rules_schema(param["rules"], f"{w}.param.rules")
            if param.get("on_failure", "fail") not in ("fail", "warn"):
                errs.append(f"{w}.param.on_failure: must be 'fail' or 'warn'")

        if kind == "cleanup":
            for k in keys or []:
                available.discard(k)
        else:
            has_output = True
            if param.get("is_separate_output_df_required"):
                out = param.get("output_df_name")
                if not isinstance(out, str) or not out:
                    errs.append(f"{w}.param.output_df_name: required when is_separate_output_df_required is true")
                else:
                    available.add(out)

    # ---------------- targets
    for i, t in enumerate(cfg["targets"]):
        w = f"targets[{i}]"
        if not isinstance(t, dict):
            errs.append(f"{w}: must be a mapping"); continue
        if not isinstance(t.get("name"), str) or not t["name"].strip():
            errs.append(f"{w}.name: required string")
        ref = t.get("persistor", "default_output")
        if not _resolvable(ref, aliases):
            errs.append(f"{w}.persistor: cannot resolve {ref!r}"); continue
        param = t.get("param") or {}
        missing = [k for k in TARGET_REQUIRED.get(_canonical(ref, aliases), []) if param.get(k) in (None, "", [], {})]
        if missing:
            errs.append(f"{w}.param: missing required {missing}")
        k = param.get("input_key")
        if k and k not in available and not (k == "output" and has_output):
            errs.append(f"{w}.param.input_key: {k!r} is not produced by any earlier step")
        _check_bools(param, f"{w}.param", errs)
        if _canonical(ref, aliases) == "scd2":
            mode = param.get("delete_detection", "none")
            if mode not in ("none", "full_snapshot", "source_flag"):
                errs.append(f"{w}.param.delete_detection: must be none | full_snapshot | source_flag")
            if mode == "source_flag" and not param.get("delete_flag_column"):
                errs.append(f"{w}.param.delete_flag_column: required when delete_detection is source_flag")
            if mode == "full_snapshot" and any((s_.get("incremental") or {}).get("enabled") for s_ in cfg["sources"]):
                errs.append(f"{w}.param.delete_detection: full_snapshot cannot be used with incremental sources "
                            "(missing keys just mean 'no new data'); use source_flag instead")
            ratio = param.get("max_delete_ratio")
            if ratio is not None and not (isinstance(ratio, (int, float)) and not isinstance(ratio, bool) and 0 <= ratio <= 1):
                errs.append(f"{w}.param.max_delete_ratio: must be a number between 0 and 1")
        for key in ("persistence_strategy", "merge_persistence_strategy"):
            if key in param and not _resolvable(param[key], aliases):
                errs.append(f"{w}.param.{key}: cannot resolve {param[key]!r}")
    return errs
