"""
Run the framework against a real Snowflake account (database FRAMEWORK_TEST from snowflake_test/01_setup.sql).

    export SNOWFLAKE_ACCOUNT=myorg-myaccount SNOWFLAKE_USER=me SNOWFLAKE_ROLE=SYSADMIN
    export SNOWFLAKE_PASSWORD=...        # omit to sign in through the browser (SSO)
    python run_local.py --ping           # check the connection
    python run_local.py                  # run config/pipeline.yml, then print target + watermark tables
    python run_local.py --show           # just print the tables
    python run_local.py --debug          # DEBUG logs (sample rows after each step)
"""
import argparse
import os
import sys

VARIABLES = {
    "SCHEMA_STG_INVY": "STG_INVY",
    "SCHEMA_RAW_INV_WEEKLY": "RAW_INV_WEEKLY",
    "SCHEMA_CON_INV_WEEKLY": "CON_INV_WEEKLY",
}


def make_session():
    from snowflake.snowpark import Session
    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER") if not os.getenv(k)]
    if missing:
        sys.exit(f"Set environment variable(s): {', '.join(missing)}")
    cfg = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "FRAMEWORK_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "FRAMEWORK_TEST"),
        "schema": "STG_INVY",          # cache_result() creates its temp tables here
    }
    if os.getenv("SNOWFLAKE_PASSWORD"):
        cfg["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        cfg["authenticator"] = "externalbrowser"
    return Session.builder.configs(cfg).create()


def show_tables(session):
    print("\n=== TARGET: CON_INV_WEEKLY.TCFD_ENTITY_INSTRUMENT_HOLDING_METRIC ===")
    session.sql("""SELECT UNIQUE_ACTIVE_BUSINESS_KEY AS BUSINESS_KEY,
                          VALUATION_METHOD_MARKET_VALUE_USD_AMOUNT AS MV, IS_ACTIVE,
                          TO_CHAR(CREATED_DT, 'MM-DD HH24:MI:SS') AS CREATED, TO_CHAR(UPDATED_DT, 'MM-DD HH24:MI:SS') AS UPDATED
                   FROM CON_INV_WEEKLY.TCFD_ENTITY_INSTRUMENT_HOLDING_METRIC
                   ORDER BY 1, CREATED_DT""").show(50)
    print("=== WATERMARK: STG_INVY.PIPELINE_WATERMARK_STATE ===")
    try:
        session.sql("SELECT SOURCE_NAME, WATERMARK_VALUE, ROW_COUNT, RUN_ID, UPDATED_AT "
                    "FROM STG_INVY.PIPELINE_WATERMARK_STATE").show(10)
    except Exception as exc:                       # table does not exist before the first successful run
        print(f"(no watermark table yet: {str(exc).splitlines()[0]})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ping", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    if args.debug:
        os.environ["log_level"] = "DEBUG"

    session = make_session()
    if args.ping:
        session.sql("SELECT CURRENT_VERSION() AS VERSION, CURRENT_ROLE() AS ROLE, CURRENT_DATABASE() AS DB, "
                    "CURRENT_SCHEMA() AS SCHEMA, CURRENT_WAREHOUSE() AS WAREHOUSE").show()
        return
    if not args.show:
        from dataprocessorlib.config_loader import ConfigValidationError
        from dataprocessorlib.framework import Pipeline
        try:
            Pipeline(session, None, variables=VARIABLES).run()      # None -> config/pipeline.yml
        except ConfigValidationError as exc:
            print(exc)
            sys.exit(2)
        except Exception as exc:
            print(f"\nPIPELINE FAILED: {exc}")
            show_tables(session)
            sys.exit(1)
    show_tables(session)


if __name__ == "__main__":
    main()
