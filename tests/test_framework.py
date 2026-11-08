"""Runs without Snowflake: stubs snowpark if it is not installed.  pytest tests/  (or python tests/test_framework.py)"""
import io, json, os, sys, tempfile, textwrap, types
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.dirname(HERE), HERE]

STUBBED = False
try:
    import snowflake.snowpark  # noqa
except ImportError:
    STUBBED = True
    sf, sp, ex = (types.ModuleType(n) for n in ("snowflake", "snowflake.snowpark", "snowflake.snowpark.exceptions"))
    sp.Session = sp.DataFrame = object
    ex.SnowparkSQLException = type("SnowparkSQLException", (Exception,), {})
    sf.snowpark, sp.exceptions = sp, ex
    sys.modules.update({"snowflake": sf, "snowflake.snowpark": sp, "snowflake.snowpark.exceptions": ex})
    import fake_snowpark; fake_snowpark.install()

from dataprocessorlib.config_loader import ConfigLoader, ConfigValidationError, PipelineConfig
from dataprocessorlib.core.base import BaseProcessor
from dataprocessorlib.framework import Pipeline
from dataprocessorlib.incremental.config import IncrementalConfig
from dataprocessorlib.utils.logger import get_json_logger
from dataprocessorlib.validation import validate_rules_schema

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VARS = {"SCHEMA_STG_INVY": "S", "SCHEMA_RAW_INV_WEEKLY": "R", "SCHEMA_CON_INV_WEEKLY": "C"}


def write(text):
    f = tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False)
    f.write(textwrap.dedent(text)); f.close()
    return f.name


def errors_for(text, variables=None):
    try:
        ConfigLoader.load(write(text), variables or {}, use_env=False)
    except ConfigValidationError as e:
        return e.errors
    return []


def test_shipped_config_is_valid():
    cfg = ConfigLoader.load(os.path.join(ROOT, "config", "pipeline.yml"), VARS)
    assert len(cfg["processors"]) == 9 and cfg["sources"][0]["name"] == "R.TCFD_INSTRUMENT_LEVEL_RAW"


def test_missing_variable_reported():
    try:
        ConfigLoader.load(os.path.join(ROOT, "config", "pipeline.yml"), {}, use_env=False)
        assert False
    except ConfigValidationError as e:
        assert any("unresolved variables" in m and "SCHEMA_STG_INVY" in m for m in e.errors)


def test_all_problems_reported_at_once():
    errs = errors_for("""
        sources:
          - name: T
            loader: nope.Missing
            param: {output_df_name: a}
          - name: T2
            param: {output_df_name: a, filter_expression: "col('X').__class__"}
        processors:
          - {id: p1, class: hash_key, param: {input_keys: [ghost], hash_column: [A]}}
          - {id: p1, class: join, param: {input_keys: [a], join_on: [K]}}
        targets:
          - {name: X, persistor: scd2, param: {input_key: zzz}}
        bogus: 1
    """)
    text = "\n".join(errs)
    for needle in ["unknown top-level key", "loader: cannot resolve", "duplicate dataframe name",
                   "unsafe or unbalanced", "missing required ['hash_key_name']", "'ghost' is not produced",
                   "duplicate id", "join needs at least 2", "missing required", "'zzz' is not produced"]:
        assert needle in text, needle


def test_duplicate_yaml_key_and_bad_yaml():
    assert any("duplicate key" in e for e in errors_for("sources: []\nsources: []\n"))
    assert any("invalid YAML" in e for e in errors_for("a: [unclosed"))
    assert any("mapping keys must be simple" in e for e in errors_for("{{a: 1}: 2}\n"))


def test_incremental_and_rules_schema():
    assert any("watermark_table" in e for e in errors_for("""
        sources: [{name: T, param: {output_df_name: a}, incremental: {enabled: true, watermark_column: C}}]
        targets: [{name: X, persistor: default_output, param: {input_key: a}}]
    """))
    assert validate_rules_schema({"not_blank": ["A"]}) == [] and validate_rules_schema({"not_blank": "A"})
    assert validate_rules_schema({"not_null": ["A"], "row_count": {"min": 1}, "unique": ["A", ["B", "C"]]}) == []
    bad = validate_rules_schema({"row_count": {"min": 5, "max": 1}, "wat": 1, "expressions": [{"name": "x"}]})
    assert len(bad) == 3
    assert IncrementalConfig.from_dict({"enabled": False}).enabled is False


def test_json_logger():
    log = get_json_logger("t.json", level="INFO")
    buf = io.StringIO(); log.handlers[0].stream = buf
    log.info("hello", extra={"job_name": "j1", "rows": 3})
    rec = json.loads(buf.getvalue())
    assert rec["message"] == "hello" and rec["job_name"] == "j1" and rec["rows"] == 3 and rec["level"] == "INFO"


def test_pipeline_config_class():
    cfg = PipelineConfig.from_yaml(os.path.join(ROOT, "config", "pipeline.yml"), VARS)
    assert cfg.pipeline_name == "tcfd_entity_instrument_holding_metric"
    src = cfg.source("R.TCFD_INSTRUMENT_LEVEL_RAW")
    assert src.output_df_name == "tcfd_instrument_level_raw" and src.incremental.enabled
    assert src.incremental.watermark_table == "S.PIPELINE_WATERMARK_STATE"
    assert cfg.processor("hash_source").param["hash_key_name"] == "SRC_HASH_KEY"
    assert cfg.processor("hash_source").cls == "hash_key" and cfg.target("C.TCFD_ENTITY_INSTRUMENT_HOLDING_METRIC").persistor == "scd2"
    try:
        cfg.processor("nope"); assert False
    except KeyError:
        pass
    # round trip: export -> reload -> same structure
    path = write(cfg.to_yaml())
    again = PipelineConfig.from_yaml(path, {}, use_env=False)
    assert again.to_dict() == cfg.to_dict()


def test_default_config_provider():
    from dataprocessorlib.config_loader import DEFAULT_CONFIG_PATH, get_config, reset_config_cache
    assert DEFAULT_CONFIG_PATH.endswith(os.path.join("config", "pipeline.yml")) and os.path.isfile(DEFAULT_CONFIG_PATH)
    reset_config_cache()
    a = get_config(variables=VARS)
    assert a is get_config(variables=VARS) and a is not get_config(variables=VARS, refresh=True)


def _df():
    from fake_snowpark import FakeDF
    return FakeDF([{"ID": 1, "NAME": "a", "AMT": 10, "TYPE": "x"},
                   {"ID": 1, "NAME": " ", "AMT": -5, "TYPE": "x"},
                   {"ID": None, "NAME": None, "AMT": 50, "TYPE": "bad"},
                   {"ID": 4, "NAME": "d", "AMT": 20, "TYPE": "y"}])


def test_core_null_and_blank_checks():
    if not STUBBED: return
    from dataprocessorlib.core.data_validation import check_nulls, null_counts
    r = {x.rule: x for x in check_nulls(_df(), ["ID", "NAME", "nope"])}
    assert r["not_null[ID]"].failed_count == 1 and r["not_null[NAME]"].failed_count == 1
    assert not r["not_null[nope]"].passed and "not found" in r["not_null[nope]"].detail
    assert null_counts(_df(), ["NAME"], include_blank=True) == {"NAME": 2}          # None + " "
    assert check_nulls(_df(), ["NAME"], include_blank=True)[0].rule == "not_blank[NAME]"


def test_core_duplicate_checks():
    if not STUBBED: return
    from dataprocessorlib.core.data_validation import check_duplicates, duplicate_count, find_duplicates
    assert duplicate_count(_df(), ["ID"]) == 1 and duplicate_count(_df()) == 0     # whole-row: none
    assert check_duplicates(_df(), ["ID"]).passed is False
    assert check_duplicates(_df(), ["TYPE", "AMT"]).passed is True
    assert not check_duplicates(_df(), ["GHOST"]).passed
    assert find_duplicates(_df(), ["ID"]).collect()[0]["OCCURRENCES"] == 2


def test_core_other_checks_and_enforce():
    if not STUBBED: return
    from dataprocessorlib.core import data_validation as dv
    df = _df()
    assert dv.check_required_columns(df, ["ID", "X"]).detail == "missing columns: ['X']"
    assert dv.check_row_count(df, min=1, max=10).passed and not dv.check_row_count(df, min=5).passed
    assert dv.check_allowed_values(df, "TYPE", ["x", "y"]).failed_count == 1        # 'bad'
    assert dv.check_range(df, "AMT", min=0, max=30).failed_count == 2               # -5 and 50
    assert dv.check_expression(df, "neg", "AMT < 0").failed_count == 1
    warn = dv.check_range(df, "AMT", min=0, severity="warn")
    assert dv.enforce_results([warn], "d") and True                                  # warn only -> no raise
    try:
        dv.enforce_results([warn, dv.check_duplicates(df, ["ID"])], "d"); assert False
    except dv.DataValidationError as e:
        assert len(e.failures) == 1 and "unique[ID]" in str(e)
    res = dv.validate_dataframe(df, "d", required_columns=["ID"], not_null=["AMT"], unique=[["TYPE", "AMT"]], enforce=False)
    assert all(r.passed for r in res) and len(res) == 3


def test_config_driven_validator_single_pass():
    if not STUBBED: return
    from dataprocessorlib.validation import DataValidationError, DataValidator
    rules = {"required_columns": ["ID"], "not_null": ["ID"], "not_blank": ["NAME"],
             "allowed_values": {"TYPE": ["x", "y"]}, "ranges": {"AMT": {"min": 0}},
             "unique": ["ID"], "row_count": {"min": 1}, "expressions": [{"name": "big", "violation": "AMT > 40", "severity": "warn"}]}
    res = {r.rule: r for r in DataValidator(_df(), rules).run()}
    assert res["not_null[ID]"].failed_count == 1 and res["not_blank[NAME]"].failed_count == 2
    assert res["allowed_values[TYPE]"].failed_count == 1 and res["ranges[AMT]"].failed_count == 1
    assert res["unique[ID]"].failed_count == 1 and res["row_count"].passed
    assert res["expression[big]"].severity == "warn" and res["expression[big]"].failed_count == 1
    try:
        DataValidator(_df(), rules).run_and_enforce(); assert False
    except DataValidationError:
        pass
    DataValidator(_df(), {"expressions": [{"name": "big", "violation": "AMT > 40"}]}, default_severity="warn").run_and_enforce()


def test_incremental_validation_support():
    if not STUBBED: return
    from fake_snowpark import FakeDF
    from dataprocessorlib.core import data_validation as dv
    from dataprocessorlib.processors.data_validation_processor import DataValidationProcessor
    from dataprocessorlib.validation import DataValidator, validate_rules_schema

    class Sess:
        def table(self, name): return FakeDF([{"ID": 1, "IS_ACTIVE": True}, {"ID": 9, "IS_ACTIVE": False}])
    batch = FakeDF([{"ID": 1}, {"ID": 2}, {"ID": 9}]); batch.session = Sess()
    assert dv.check_keys_not_in_target(batch, "T", ["ID"]).failed_count == 2          # 1 and 9 already loaded
    assert dv.check_keys_not_in_target(batch, "T", ["ID"], "IS_ACTIVE == True").failed_count == 1
    assert dv.check_keys_not_in_target(batch, "T", ["ID"], severity="warn").severity == "warn"
    assert not dv.check_keys_not_in_target(batch, "T", ["GHOST"]).passed
    rules = {"keys_not_in_target": [{"table": "T", "keys": ["ID"], "filter": "IS_ACTIVE == True"}]}
    assert validate_rules_schema(rules) == [] and validate_rules_schema({"keys_not_in_target": [{"table": "T"}]})
    assert DataValidator(batch, rules).run()[0].rule == "keys_not_in_target[T]"

    empty = FakeDF([], ["ID"])                      # incremental slice with no new rows
    strict = {"row_count": {"min": 1}}
    dfs = {"slice": empty}
    try:
        DataValidationProcessor().transform(dfs, input_keys=["slice"], rules=strict); assert False
    except dv.DataValidationError:
        pass
    assert DataValidationProcessor().transform(dfs, input_keys=["slice"], rules=strict, skip_if_empty=True) == dfs


def _scd2(rows, session=None, **kw):
    """Run SCD2TypeOutput against fakes; returns the recorded events."""
    if not STUBBED: return None
    from fake_snowpark import EVENTS, FakeDF, FakeBulk, FakeSession
    from dataprocessorlib.target.scd2_type_output import SCD2TypeOutput
    EVENTS.clear(); FakeBulk.fail = kw.pop("fail_insert", False)
    base = dict(input_key="in", key_columns=["K"], source_hash_key="SH", target_hash_key="TH",
                effective_to="END_DT", current_flag="IS_ACTIVE", updated_date="END_DT", updated_by="UPD_BY",
                default_columns={"CREATED_DT": "expr: now"},
                persistence_strategy="fake_snowpark.FakeBulk", merge_persistence_strategy="fake_snowpark.FakeMerge")
    try:
        SCD2TypeOutput(session or FakeSession()).persist({"in": FakeDF(rows)}, "TGT", **{**base, **kw})
    finally:
        FakeBulk.fail = False
    return list(EVENTS)


def test_scd2_changed_new_unchanged_in_one_transaction():
    if not STUBBED: return
    ev = _scd2([{"K": "K1", "V": 2, "SH": "h2", "TH": "h1"},      # changed
                {"K": "K2", "V": 5, "SH": "h",  "TH": None},      # new key
                {"K": "K3", "V": 3, "SH": "h",  "TH": "h"}])      # unchanged
    kinds = [e[0] if e[0] != "sql" else e[1] for e in ev]
    assert kinds == ["BEGIN", "merge", "insert", "COMMIT"]
    assert ev[1][1] == ["K1", "K2"] and {"IS_ACTIVE", "END_DT", "UPD_BY"} <= set(ev[1][2])
    inserted = ev[2][1]
    assert [r["K"] for r in inserted] == ["K1", "K2"] and all(r["IS_ACTIVE"] is True for r in inserted)
    assert all("SH" not in r and "TH" not in r and "CREATED_DT" in r for r in inserted)


def test_scd2_nothing_to_do_and_rollback_and_no_transaction():
    if not STUBBED: return
    from fake_snowpark import EVENTS, FakeBulk
    assert _scd2([{"K": "K1", "SH": "h", "TH": "h"}]) == []                        # no BEGIN at all
    rows = [{"K": "K1", "SH": "a", "TH": "b"}]
    try:
        _scd2(rows, fail_insert=True); assert False
    except RuntimeError as e:
        assert "insert blew up" in str(e)
        assert [x[1] if x[0] == "sql" else x[0] for x in EVENTS] == ["BEGIN", "merge", "ROLLBACK"]
    ev = _scd2(rows, use_transaction=False)
    assert [e[0] for e in ev] == ["merge", "insert"]                                # no BEGIN/COMMIT


def test_scd2_soft_delete_source_flag():
    if not STUBBED: return
    ev = _scd2([{"K": "K1", "SH": "x", "TH": "x", "IS_DELETED": True},            # delete (even though hash equal)
                {"K": "K2", "SH": "n", "TH": "o", "IS_DELETED": False},           # normal change
                {"K": "K3", "SH": "n", "TH": None, "IS_DELETED": None}],          # new, null flag = live
               delete_detection="source_flag", delete_flag_column="IS_DELETED", deleted_flag_column="IS_GONE")
    merges = [e for e in ev if e[0] == "merge"]
    assert merges[0][1] == ["K2", "K3"] and "IS_GONE" not in merges[0][2]
    assert merges[1][1] == ["K1"] and "IS_GONE" in merges[1][2]                    # delete marks IS_GONE
    ins = [e for e in ev if e[0] == "insert"][0][1]
    assert [r["K"] for r in ins] == ["K2", "K3"] and all("IS_DELETED" not in r for r in ins)
    assert ev[0] == ("sql", "BEGIN") and ev[-1] == ("sql", "COMMIT")


def test_scd2_soft_delete_full_snapshot_and_guards():
    if not STUBBED: return
    from fake_snowpark import FakeDF, FakeSession
    tgt = lambda: FakeSession({"TGT": FakeDF([{"K": k, "IS_ACTIVE": True} for k in ("K1", "K2", "K3", "K4")]
                                              + [{"K": "OLD", "IS_ACTIVE": False}])})
    src = [{"K": "K1", "SH": "a", "TH": "a"}, {"K": "K2", "SH": "a", "TH": "a"}, {"K": "K3", "SH": "a", "TH": "a"}]
    ev = _scd2(src, tgt(), delete_detection="full_snapshot")                        # K4 missing: 1/4 <= 0.5
    merges = [e for e in ev if e[0] == "merge"]
    assert len(merges) == 1 and merges[0][1] == ["K4"] and not [e for e in ev if e[0] == "insert"]
    try:
        _scd2(src, tgt(), delete_detection="full_snapshot", max_delete_ratio=0.1); assert False
    except ValueError as e:
        assert "max_delete_ratio" in str(e)
    try:
        _scd2([], tgt(), delete_detection="full_snapshot"); assert False
    except ValueError as e:
        assert "empty source" in str(e)
    try:
        _scd2(src, tgt(), delete_detection="bogus"); assert False
    except ValueError as e:
        assert "delete_detection" in str(e)


def test_scd2_delete_config_validation():
    base = """
        sources: [{name: T, param: {output_df_name: a}, incremental: INC}]
        targets: [{name: X, persistor: scd2, param: {input_key: a, key_columns: [K], source_hash_key: S,
                   target_hash_key: T, effective_to: E, current_flag: F, DELETE}}]
    """
    inc = "{enabled: true, watermark_column: C, watermark_table: W}"
    bad = errors_for(base.replace("INC", inc).replace("DELETE", "delete_detection: full_snapshot, max_delete_ratio: 2"))
    assert any("cannot be used with incremental" in e for e in bad) and any("max_delete_ratio" in e for e in bad)
    assert any("delete_flag_column" in e for e in errors_for(base.replace("INC", inc).replace("DELETE", "delete_detection: source_flag")))
    assert errors_for(base.replace("INC", inc).replace("DELETE", "delete_detection: source_flag, delete_flag_column: D")) == []
    assert errors_for(base.replace("INC", "{enabled: false}").replace("DELETE", "delete_detection: full_snapshot")) == []
    assert any("must be none" in e for e in errors_for(base.replace("INC", "{enabled: false}").replace("DELETE", "delete_detection: x")))


def test_transaction_context_manager():
    from dataprocessorlib.core.session_mixin import SessionMixin
    stmts = []
    sess = types.SimpleNamespace(sql=lambda q: (stmts.append(q), types.SimpleNamespace(collect=lambda: []))[1])
    m = SessionMixin(sess)
    with m.transaction(): pass
    try:
        with m.transaction(): raise KeyError("x")
    except KeyError: pass
    assert stmts == ["BEGIN", "COMMIT", "BEGIN", "ROLLBACK"]


class FakeLoader:
    def __init__(self, session): pass
    def load(self, name, **kw): return {"rows": [1, 2, 3]}

class FakeProc(BaseProcessor):
    def transform(self, dfs, **kw):
        return self._build_output_dict(dfs, {"rows": len(dfs["a"]["rows"])}, kw.get("is_separate_output_df_required"), kw.get("output_df_name"))

class FakeOut:
    ok = True
    calls = []
    def __init__(self, session): pass
    def persist(self, dfs, target_name, **kw):
        FakeOut.calls.append((target_name, dfs[kw["input_key"]])); return FakeOut.ok


def test_pipeline_orchestration_and_commit_rules():
    cfg = write("""
        sources: [{name: T, loader: test_framework.FakeLoader, param: {output_df_name: a}}]
        processors: [{id: p, class: test_framework.FakeProc, param: {input_keys: [a], is_separate_output_df_required: true, output_df_name: b}}]
        targets: [{name: OUT, persistor: test_framework.FakeOut, param: {input_key: b}}]
    """)
    sys.modules.setdefault("tests.test_framework", sys.modules[__name__])
    p = Pipeline(session=None, config_path=cfg)
    dfs = p.run()
    assert dfs["b"] == {"rows": 3} and dfs["output"] == {"rows": 3} and FakeOut.calls[-1][0] == "OUT"
    FakeOut.ok = False
    try:
        Pipeline(None, cfg).run(); assert False
    except RuntimeError as e:
        assert "persist returned False" in str(e)
    finally:
        FakeOut.ok = True


if __name__ == "__main__":
    import test_framework as t          # import by name so dotted class paths resolve to the same classes
    for name in [n for n in dir(t) if n.startswith("test_")]:
        getattr(t, name)(); print("PASS", name)
