"""
Typed view of the pipeline YAML.

    cfg = PipelineConfig.from_yaml("config/pipeline.yml", variables={...})   # validated first
    cfg.source("R.TCFD_INSTRUMENT_LEVEL_RAW").incremental.watermark_column
    cfg.processor("hash_source").param["hash_key_name"]
    cfg.to_yaml("rendered.yml")                                              # variables resolved
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import yaml

from dataprocessorlib.config_loader.loader import ConfigLoader
from dataprocessorlib.incremental.config import IncrementalConfig


@dataclass
class MetadataConfig:
    pipeline_name: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)       # schema_stg, etc.

    @classmethod
    def from_dict(cls, d: Optional[dict]) -> "MetadataConfig":
        d = dict(d or {})
        return cls(pipeline_name=d.pop("pipeline_name", None), extra=d)

    def to_dict(self) -> dict:
        return {**({"pipeline_name": self.pipeline_name} if self.pipeline_name else {}), **self.extra}


@dataclass
class SourceConfig:
    name: str
    loader: str = "default"
    param: Dict[str, Any] = field(default_factory=dict)
    incremental: IncrementalConfig = field(default_factory=IncrementalConfig)
    enabled: bool = True

    @property
    def output_df_name(self) -> str:
        return self.param["output_df_name"]

    @classmethod
    def from_dict(cls, d: dict) -> "SourceConfig":
        return cls(name=d["name"], loader=d.get("loader", "default"), param=dict(d.get("param") or {}),
                   incremental=IncrementalConfig.from_dict(d.get("incremental")), enabled=d.get("enabled", True))

    def to_dict(self) -> dict:
        out = {"name": self.name, "loader": self.loader, "param": self.param}
        if self.incremental.enabled:
            out["incremental"] = asdict(self.incremental)
        if not self.enabled:
            out["enabled"] = False
        return out


@dataclass
class ProcessorConfig:
    cls: str                                   # YAML key is `class`
    id: Optional[str] = None
    param: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    @property
    def label(self) -> str:
        return self.id or self.cls

    @classmethod
    def from_dict(cls, d: dict) -> "ProcessorConfig":
        return cls(cls=d["class"], id=d.get("id"), param=dict(d.get("param") or {}), enabled=d.get("enabled", True))

    def to_dict(self) -> dict:
        out = ({"id": self.id} if self.id else {}) | {"class": self.cls, "param": self.param}
        return out | ({} if self.enabled else {"enabled": False})


@dataclass
class TargetConfig:
    name: str
    persistor: str = "default_output"
    param: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "TargetConfig":
        return cls(name=d["name"], persistor=d.get("persistor", "default_output"),
                   param=dict(d.get("param") or {}), enabled=d.get("enabled", True))

    def to_dict(self) -> dict:
        out = {"name": self.name, "persistor": self.persistor, "param": self.param}
        return out | ({} if self.enabled else {"enabled": False})


@dataclass
class PipelineConfig:
    metadata: MetadataConfig
    sources: List[SourceConfig]
    processors: List[ProcessorConfig]
    targets: List[TargetConfig]

    # ---- construction -------------------------------------------------
    @classmethod
    def from_dict(cls, cfg: dict) -> "PipelineConfig":
        """`cfg` must already be validated (use from_yaml for the full path)."""
        return cls(metadata=MetadataConfig.from_dict(cfg.get("metadata")),
                   sources=[SourceConfig.from_dict(s) for s in cfg["sources"]],
                   processors=[ProcessorConfig.from_dict(p) for p in cfg.get("processors") or []],
                   targets=[TargetConfig.from_dict(t) for t in cfg["targets"]])

    @classmethod
    def from_yaml(cls, path: str, variables: Optional[dict] = None, aliases: Optional[dict] = None,
                  use_env: bool = True) -> "PipelineConfig":
        """Load + render variables + validate everything; raises ConfigValidationError."""
        return cls.from_dict(ConfigLoader.load(path, variables, aliases, use_env))

    # ---- lookups ------------------------------------------------------
    def source(self, name: str) -> SourceConfig:
        return self._find(self.sources, lambda s: s.name == name, f"source {name!r}")

    def processor(self, id_: str) -> ProcessorConfig:
        return self._find(self.processors, lambda p: p.id == id_, f"processor {id_!r}")

    def target(self, name: str) -> TargetConfig:
        return self._find(self.targets, lambda t: t.name == name, f"target {name!r}")

    @staticmethod
    def _find(items, pred, what):
        for i in items:
            if pred(i):
                return i
        raise KeyError(f"{what} not found in config")

    @property
    def pipeline_name(self) -> Optional[str]:
        return self.metadata.pipeline_name

    # ---- export -------------------------------------------------------
    def to_dict(self) -> dict:
        return {"metadata": self.metadata.to_dict(),
                "sources": [s.to_dict() for s in self.sources],
                "processors": [p.to_dict() for p in self.processors],
                "targets": [t.to_dict() for t in self.targets]}

    def to_yaml(self, path: Optional[str] = None) -> str:
        text = yaml.safe_dump(self.to_dict(), sort_keys=False, default_flow_style=False)
        if path:
            with open(path, "w") as f:
                f.write(text)
        return text
