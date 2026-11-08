from functools import reduce

from dataprocessorlib.processors.template_processor import TemplateProcessor


class UnionProcessor(TemplateProcessor):
    """Union all input_keys by column name (union_all=False removes duplicates)."""
    NEEDS_SINGLE_DF = False

    def run(self, selected, p, job):
        frames = list(selected.values())
        if len(frames) < 2:
            raise ValueError("UnionProcessor needs at least two input_keys")
        op = "union_all_by_name" if p.get("union_all", True) else "union_by_name"
        return reduce(lambda a, b: getattr(a, op)(b), frames)
