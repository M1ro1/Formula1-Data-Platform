"""Snowflake connection helper. Reads settings from environment variables."""
import os

import snowflake.connector

def get_connection(**overrides):
    params = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "role": os.getenv("SNOWFLAKE_ROLE", "F1_LOADER"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "F1_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "F1_DWH"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    }

    params.update(overrides)
    return snowflake.connector.connect(**params)


if __name__ == "__main__":
    # Smoke test:  python -m include.snowflake_conn
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
            "CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        print(cur.fetchone())
