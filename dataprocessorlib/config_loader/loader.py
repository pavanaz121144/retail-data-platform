"""Load a pipeline YAML: duplicate-key check, {{ VAR }} rendering, full validation."""
from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Set

import yaml

from dataprocessorlib.config_loader.errors import ConfigValidationError
from dataprocessorlib.config_loader.schema import validate_config
from dataprocessorlib.core.registry import ALIASES

_VAR = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys (YAML silently keeps the last one)."""

    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError:
                raise yaml.constructor.ConstructorError(
                    None, None, "mapping keys must be simple values, not lists or mappings", key_node.start_mark)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"duplicate key {key!r}", key_node.start_mark)
            seen.add(key)
        return super().construct_mapping(node, deep)


def _render(obj: Any, lookup: Dict[str, str], missing: Set[str]) -> Any:
    if isinstance(obj, str):
        def sub(m):
            if m.group(1) in lookup:
                return str(lookup[m.group(1)])
            missing.add(m.group(1))
            return m.group(0)
        return _VAR.sub(sub, obj)
    if isinstance(obj, list):
        return [_render(i, lookup, missing) for i in obj]
    if isinstance(obj, dict):
        return {k: _render(v, lookup, missing) for k, v in obj.items()}
    return obj


class ConfigLoader:
    @staticmethod
    def load(path: str, variables: Optional[Dict[str, str]] = None,
             aliases: Optional[Dict[str, str]] = None, use_env: bool = True) -> dict:
        p = Path(path)
        if not p.is_file():
            raise ConfigValidationError([f"config file not found: {path}"])
        try:
            raw = yaml.load(p.read_text(), Loader=_UniqueKeyLoader)
        except yaml.YAMLError as exc:
            raise ConfigValidationError([f"invalid YAML: {exc}"]) from exc
        if raw is None:
            raise ConfigValidationError(["config file is empty"])

        lookup = {**(os.environ if use_env else {}), **(variables or {})}
        missing: Set[str] = set()
        cfg = _render(raw, lookup, missing)

        errors = []
        if missing:
            errors.append(f"unresolved variables (pass them in `variables`): {sorted(missing)}")
        errors += validate_config(cfg, {**ALIASES, **(aliases or {})})
        if errors:
            raise ConfigValidationError(errors)
        return cfg
