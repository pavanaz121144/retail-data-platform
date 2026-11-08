from dataprocessorlib.processors.template_processor import TemplateProcessor


class CacheProcessor(TemplateProcessor):
    """Materialise a DataFrame (temp table) so later steps don't recompute it."""

    def run(self, selected, p, job):
        return self.single(selected).cache_result()
