import uuid
from datetime import datetime

from dataprocessorlib.processors.template_processor import TemplateProcessor


class BatchIdGeneratorProcessor(TemplateProcessor):
    """Stamp BATCH_ID / JOB_ID columns; values from params, else generated."""
    REQUIRED = ("input_keys", "batch_id_key", "job_id_key")

    def run(self, selected, p, job):
        from snowflake.snowpark.functions import lit
        batch = p.get("batch_id") or datetime.utcnow().strftime("%Y%m%d%H%M%S")
        run_job = p.get("job_id") or str(uuid.uuid4())
        return (self.single(selected)
                .with_column(p["batch_id_key"], lit(batch))
                .with_column(p["job_id_key"], lit(run_job)))
