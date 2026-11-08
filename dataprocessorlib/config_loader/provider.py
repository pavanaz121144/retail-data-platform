"""
One place every util/class goes to read config/pipeline.yml.

    from dataprocessorlib.config_loader import get_config
    cfg = get_config(variables={...})            # default: <project root>/config/pipeline.yml, cached
    cfg.processor("hash_source").param
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Optional

from dataprocessorlib.config_loader.models import PipelineConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = str(PROJECT_ROOT / "config" / "pipeline.yml")
_CACHE: Dict[str, PipelineConfig] = {}


def get_config(path: Optional[str] = None, variables: Optional[dict] = None,
               aliases: Optional[dict] = None, refresh: bool = False) -> PipelineConfig:
    """Validated PipelineConfig, loaded once per (path, variables) and then cached."""
    path = str(path or DEFAULT_CONFIG_PATH)
    key = f"{path}|{json.dumps(variables or {}, sort_keys=True)}"
    if refresh or key not in _CACHE:
        _CACHE[key] = PipelineConfig.from_yaml(path, variables, aliases)
    return _CACHE[key]


def reset_config_cache() -> None:
    _CACHE.clear()
