from dataprocessorlib.core.utils import as_list
from dataprocessorlib.processors.template_processor import TemplateProcessor


class JoinProcessor(TemplateProcessor):
    """
    Join input_keys[0] with the rest.
      join_on:    list (same for all) or {df_key: [cols]}
      join_types: {df_key: 'inner'|'left'|...}  (default inner)
    """
    REQUIRED = ("input_keys", "join_on")
    NEEDS_SINGLE_DF = False

    def run(self, selected, p, job):
        keys = list(selected)
        if len(keys) < 2:
            raise ValueError("JoinProcessor needs at least two input_keys")
        result = selected[keys[0]]
        join_on, types = p["join_on"], p.get("join_types") or {}
        for k in keys[1:]:
            on = join_on.get(k) if isinstance(join_on, dict) else join_on
            if not on:
                raise ValueError(f"No join_on columns given for '{k}'")
            result = result.join(selected[k], on=as_list(on), how=types.get(k, "inner"))
        result = self._select_input_columns(result, p.get("include_columns"), p.get("exclude_columns"))
        result = self._apply_column_mapping(result, p.get("column_mapping"), job)
        return result.distinct() if p.get("is_distinct") else result
