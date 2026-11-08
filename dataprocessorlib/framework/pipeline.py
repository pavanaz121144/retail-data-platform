"""
Config-driven runner. The YAML is loaded into a typed PipelineConfig (validated up front), then:
  sources    : <loader>(session).load(name, **param)           -> incremental slice -> dfs[output_df_name]
  processors : <class>(session).transform(dfs, **param)        -> dfs (dict with 'output' + named outputs)
  targets    : <persistor>(session).persist(dfs, name, **param) -> bool (False raises)
Watermarks are committed only after every target succeeds.
"""
from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Dict, Optional

from dataprocessorlib.config_loader import DEFAULT_CONFIG_PATH, PipelineConfig
from dataprocessorlib.core.registry import resolve_class
from dataprocessorlib.incremental import IncrementalManager
from dataprocessorlib.utils.logger import get_json_logger

Hook = Callable[[str, Any, Dict[str, Any]], None]    # (label, SourceConfig|ProcessorConfig|TargetConfig, dfs)


class Pipeline:
    def __init__(self, session, config_path: Optional[str] = None, variables: Optional[dict] = None,
                 aliases: Optional[dict] = None, before_step: Optional[Hook] = None,
                 after_step: Optional[Hook] = None):
        self.log = get_json_logger("dataprocessorlib.pipeline")
        self.session, self.aliases = session, aliases or {}
        config_path = config_path or DEFAULT_CONFIG_PATH
        self.config = PipelineConfig.from_yaml(config_path, variables, self.aliases)   # raises ConfigValidationError
        self.before_step, self.after_step = before_step, after_step
        self.run_id = str(uuid.uuid4())
        self.incremental = IncrementalManager(session, pipeline_name=self.config.pipeline_name or config_path,
                                              run_id=self.run_id)
        self.dfs: Dict[str, Any] = {}

    def run(self) -> Dict[str, Any]:
        t0 = time.time()
        self.log.info("Pipeline started", extra={"run_id": self.run_id})
        try:
            for s in self.config.sources:
                self._step("source", s.name, s, self._load)
            for p in self.config.processors:
                self._step("processor", p.label, p, self._process)
            for t in self.config.targets:
                self._step("target", t.name, t, self._persist)
        except Exception:
            self.incremental.discard()          # a failed run never advances watermarks
            self.log.error("Pipeline failed", extra={"run_id": self.run_id}, exc_info=True)
            raise
        self.incremental.commit()
        self.log.info("Pipeline finished", extra={"run_id": self.run_id, "seconds": round(time.time() - t0, 1)})
        return self.dfs

    # ------------------------------------------------------------------
    def _step(self, kind, name, spec, fn):
        if not spec.enabled:
            return
        label = f"{kind}:{name}"
        if self.before_step:
            self.before_step(label, spec, self.dfs)
        start = time.time()
        self.log.info(f"{label} started", extra={"run_id": self.run_id, "step": label})
        try:
            fn(spec, label)
        except Exception as exc:
            raise RuntimeError(f"Step failed [{label}]: {exc}") from exc
        self.log.info(f"{label} ok", extra={"run_id": self.run_id, "step": label,
                                              "seconds": round(time.time() - start, 2)})
        if self.after_step:
            self.after_step(label, spec, self.dfs)

    def _cls(self, ref):
        return resolve_class(ref, self.aliases)

    @staticmethod
    def _param(spec, label):
        return {"job_name": label, **spec.param}

    def _load(self, s, label):
        df = self._cls(s.loader)(self.session).load(s.name, **self._param(s, label))
        self.dfs[s.output_df_name] = self.incremental.prepare(df, s.incremental, s.name)

    def _process(self, p, label):
        self.dfs = self._cls(p.cls)(self.session).transform(self.dfs, **self._param(p, label))

    def _persist(self, t, label):
        ok = self._cls(t.persistor)(self.session).persist(self.dfs, t.name, **self._param(t, label))
        if ok is False:
            raise RuntimeError(f"persist returned False for {t.name}")
