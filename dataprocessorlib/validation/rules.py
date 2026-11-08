"""Schema for the `rules:` block of a validate step (checked at config-load time)."""
from __future__ import annotations
from typing import Any, List

ALLOWED_RULE_KEYS = {"required_columns", "not_null", "not_blank", "unique", "row_count",
                     "allowed_values", "ranges", "expressions", "keys_not_in_target"}
SEVERITIES = {"warn", "error"}


def _is_str_list(v) -> bool:
    return isinstance(v, list) and all(isinstance(i, str) for i in v)


def validate_rules_schema(rules: Any, path: str = "rules") -> List[str]:
    if not isinstance(rules, dict) or not rules:
        return [f"{path}: must be a non-empty mapping"]
    errs: List[str] = []
    for k in set(rules) - ALLOWED_RULE_KEYS:
        errs.append(f"{path}.{k}: unknown rule (allowed: {sorted(ALLOWED_RULE_KEYS)})")

    for key in ("required_columns", "not_null", "not_blank"):
        if key in rules and not _is_str_list(rules[key]):
            errs.append(f"{path}.{key}: must be a list of column names")

    if "unique" in rules:
        u = rules["unique"]
        if not isinstance(u, list) or not all(isinstance(i, str) or _is_str_list(i) for i in u):
            errs.append(f"{path}.unique: must be a list of columns or lists of columns")

    if "row_count" in rules:
        rc = rules["row_count"]
        if not isinstance(rc, dict) or not rc or set(rc) - {"min", "max"}:
            errs.append(f"{path}.row_count: use {{min: n, max: n}}")
        else:
            if not all(isinstance(v, int) and v >= 0 for v in rc.values()):
                errs.append(f"{path}.row_count: values must be non-negative integers")
            elif "min" in rc and "max" in rc and rc["min"] > rc["max"]:
                errs.append(f"{path}.row_count: min is greater than max")

    if "allowed_values" in rules:
        av = rules["allowed_values"]
        if not isinstance(av, dict) or not all(isinstance(v, list) and v for v in av.values()):
            errs.append(f"{path}.allowed_values: use {{COLUMN: [v1, v2]}} with non-empty lists")

    if "ranges" in rules:
        rg = rules["ranges"]
        if not isinstance(rg, dict) or not all(
                isinstance(v, dict) and v and not (set(v) - {"min", "max"}) for v in rg.values()):
            errs.append(f"{path}.ranges: use {{COLUMN: {{min: a, max: b}}}}")

    if "expressions" in rules:
        ex = rules["expressions"]
        if not isinstance(ex, list):
            errs.append(f"{path}.expressions: must be a list")
        else:
            for i, e in enumerate(ex):
                p = f"{path}.expressions[{i}]"
                if not isinstance(e, dict) or not isinstance(e.get("name"), str) or not isinstance(e.get("violation"), str):
                    errs.append(f"{p}: needs string 'name' and 'violation' (SQL that is TRUE for bad rows)")
                elif e.get("severity", "error") not in SEVERITIES:
                    errs.append(f"{p}.severity: must be one of {sorted(SEVERITIES)}")

    if "keys_not_in_target" in rules:
        kt = rules["keys_not_in_target"]
        if not isinstance(kt, list):
            errs.append(f"{path}.keys_not_in_target: must be a list")
        else:
            for i, item in enumerate(kt):
                p = f"{path}.keys_not_in_target[{i}]"
                if not isinstance(item, dict) or not isinstance(item.get("table"), str) or not _is_str_list(item.get("keys")) or not item.get("keys"):
                    errs.append(f"{p}: needs string 'table' and non-empty list 'keys' (optional 'filter', 'severity')")
                elif item.get("severity", "error") not in SEVERITIES:
                    errs.append(f"{p}.severity: must be one of {sorted(SEVERITIES)}")
    return errs
