"""python run_pipeline.py [config/pipeline.yml]   (defaults to config/pipeline.yml)"""
import sys

from snowflake.snowpark import Session

from dataprocessorlib.config_loader import ConfigValidationError
from dataprocessorlib.framework import Pipeline

VARIABLES = {
    "SCHEMA_STG_INVY": "STG_INVY",
    "SCHEMA_RAW_INV_WEEKLY": "RAW_INV_WEEKLY",
    "SCHEMA_CON_INV_WEEKLY": "CON_INV_WEEKLY",
}

if __name__ == "__main__":
    session = Session.builder.configs({}).create()      # fill in your connection params
    try:
        Pipeline(session, sys.argv[1] if len(sys.argv) > 1 else None, variables=VARIABLES).run()
    except ConfigValidationError as e:                  # all config problems listed at once
        print(e)
        sys.exit(2)
