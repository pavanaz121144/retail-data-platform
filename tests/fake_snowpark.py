"""Tiny in-memory stand-in for the Snowpark bits the validators use (only installed when snowpark is absent)."""
import sys, types


class Expr:
    def __init__(self, fn): self.fn = fn
    def __call__(self, row): return self.fn(row)
    def _bin(self, o, op):
        o = o if isinstance(o, Expr) else Expr(lambda r, v=o: v)
        return Expr(lambda r: op(self(r), o(r)))
    def __eq__(self, o): return self._bin(o, lambda a, b: a == b)
    def __ne__(self, o): return self._bin(o, lambda a, b: a != b)
    def __lt__(self, o): return self._bin(o, lambda a, b: a is not None and a < b)
    def __gt__(self, o): return self._bin(o, lambda a, b: a is not None and a > b)
    def __and__(self, o): return self._bin(o, lambda a, b: bool(a) and bool(b))
    def __or__(self, o): return self._bin(o, lambda a, b: bool(a) or bool(b))
    def __invert__(self): return Expr(lambda r: not self(r))
    __hash__ = None
    def is_null(self): return Expr(lambda r: self(r) is None)
    def is_not_null(self): return Expr(lambda r: self(r) is not None)
    def in_(self, vals): return Expr(lambda r: self(r) in vals)
    def cast(self, _): return Expr(lambda r: None if self(r) is None else str(self(r)))
    def desc(self): return self
    def alias(self, name): self.name = name; return self


class Agg:
    def __init__(self, kind, expr): self.kind, self.expr, self.name = kind, expr, None
    def alias(self, n): self.name = n; return self
    def compute(self, rows):
        if self.kind == "count": return len(rows)
        vals = [self.expr(r) for r in rows]
        return sum(vals) if vals else None


class Case(Expr):
    def __init__(self, cond, val): self.cond, self.val = cond, val; super().__init__(lambda r: val(r) if cond(r) else None)
    def otherwise(self, other):
        return Expr(lambda r: self.val(r) if self.cond(r) else other(r))


class Row(dict): pass


class FakeDF:
    def __init__(self, rows, columns=None):
        self.rows = rows
        self.columns = columns or (list(rows[0]) if rows else [])
    def filter(self, e): return FakeDF([r for r in self.rows if e(r)], self.columns)
    def count(self): return len(self.rows)
    session = None
    def select(self, *names): return FakeDF([{n: r[n] for n in names} for r in self.rows], list(names))
    def distinct(self):
        seen, out = set(), []
        for r in self.rows:
            k = tuple(r.items())
            if k not in seen: seen.add(k); out.append(r)
        return FakeDF(out, self.columns)
    def join(self, other, on, how="inner"):
        hit = lambda r: any(all(r[k] == o[k] for k in on) for o in other.rows)
        return FakeDF([r for r in self.rows if (not hit(r) if how == "anti" else hit(r))], self.columns)
    def cache_result(self): return self
    def drop(self, *names): return FakeDF([{k: v for k, v in r.items() if k not in names} for r in self.rows],
                                           [c for c in self.columns if c not in names])
    def with_column(self, name, e):
        return FakeDF([dict(r, **{name: e(r)}) for r in self.rows], self.columns + ([name] if name not in self.columns else []))
    def sort(self, *_): return self
    def limit(self, n): return FakeDF(self.rows[:n], self.columns)
    def collect(self): return [Row(r) for r in self.rows]
    def agg(self, *aggs):
        out = {a.name: a.compute(self.rows) for a in aggs}
        return FakeDF([out], list(out))
    def group_by(self, *keys):
        df = self
        class G:
            def agg(_, a):
                groups = {}
                for r in df.rows:
                    groups.setdefault(tuple(r[k] for k in keys), []).append(r)
                rows = [dict(zip(keys, k), **{a.name: a.compute(v)}) for k, v in groups.items()]
                return FakeDF(rows, list(keys) + [a.name])
        return G()


EVENTS = []


class FakeSession:
    def __init__(self, tables=None): self.tables = tables or {}
    def table(self, name): return self.tables[name]
    def sql(self, stmt):
        EVENTS.append(("sql", stmt))
        return types.SimpleNamespace(collect=lambda: [])


class FakeMerge:
    def __init__(self, session): pass
    def merge(self, table, source_df, keys, update_values, target_filter=None):
        EVENTS.append(("merge", sorted(r[keys[0]] for r in source_df.rows), sorted(update_values)))


class FakeBulk:
    fail = False
    def __init__(self, session): pass
    def insert(self, df, table):
        if FakeBulk.fail: raise RuntimeError("insert blew up")
        EVENTS.append(("insert", [dict(r) for r in df.rows]))


def install():
    f = types.ModuleType("snowflake.snowpark.functions")
    f.col = lambda n: Expr(lambda r: r[n])
    f.lit = lambda v: Expr(lambda r: v)
    f.trim = lambda e: Expr(lambda r: None if e(r) is None else e(r).strip())
    f.sql_expr = lambda s: Expr(lambda r: eval(s, {}, dict(r)))      # test-only: tiny SQL-ish conditions
    f.when = lambda c, v: Case(c, v)
    f.current_timestamp = lambda: Expr(lambda r: "NOW")
    f.count = lambda e: Agg("count", e)
    f.sum = lambda e: Agg("sum", e)
    sys.modules["snowflake.snowpark.functions"] = f
    sys.modules["snowflake.snowpark"].functions = f
