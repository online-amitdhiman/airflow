from __future__ import annotations

import pendulum
import base64, os
from pathlib import Path
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from src.yfinance_loader import fetch_and_load_stock_data  # Import our function

# --- Configuration ---
SF_CONN = "SF_CONN"
SF_DB = "YFINANCE"
SF_SCHEMA = "PUBLIC"
YFINANCE_TABLE = "PRICE_HISTORY"
TICKERS_TO_FETCH = ["MSFT", "AAPL", "GOOGL"]

# private_key = base64.b64decode(os.getenv("SF_FQ15637_PRIVATE_KEY"))
# dbt Configuration
PROJECT_ROOT_PATH = Path(__file__).parent.parent / "YFINANCE"

# --- /Configuration ---
@dag(
    dag_id="YFINANCE_DATA_LOAD",
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["data"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
)
def yahoo_finance_pipeline():
    ensure_schema_exist = SnowflakeSqlApiOperator(
        task_id="yfinance_table_check",
        snowflake_conn_id=SF_CONN,
        sql="""
                CREATE SCHEMA IF NOT EXISTS {SF_DB}.{SF_SCHEMA};
            """,
    )
    ensure_table_exist = SnowflakeSqlApiOperator(
        task_id="create_table",
        snowflake_conn_id=SF_CONN,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_SCHEMA}.{YFINANCE_TABLE} (
                DATE TIMESTAMP_NTZ,
                OPEN FLOAT,
                HIGH FLOAT,
                LOW FLOAT,
                CLOSE FLOAT,
                ADJ_CLOSE FLOAT,
                VOLUME NUMBER,
                DIVIDENDS FLOAT,
                STOCK_SPLITS FLOAT,
                TICKER VARCHAR,
                LOADTIMESTAMP TIMESTAMP_NTZ
    );
    """,
    )

    @task
    def extract_load_yahoo_finance(
        tickers: list[str],
        conn_id: str,
        db: str,
        schema: str,
        table: str,
        logical_date,  # Airflow injects this!
    ):
        """
        Task to extract data for the previous day and load it into Snowflake.
        """
        # Calculate start and end dates for the previous day based on logical_date
        # logical_date is the *start* of the DAG run interval
        end_date_str = logical_date
        start_date = pendulum.from_format(logical_date, "YYYY-MM-DD", tz="UTC")
        start_date = start_date.subtract(days=365)  # Use subtract for clarity
        start_date_str = start_date.to_date_string()
        print(
            f"Fetching data from {start_date_str} up to (but not including) {end_date_str}"
        )

        fetch_and_load_stock_data(
            tickers=tickers,
            snowflake_conn_id=conn_id,
            database=db,
            schema=schema,
            table_name=table,
            start_date_str=start_date_str,
            end_date_str=end_date_str,
        )

    # Task to run the extraction and loading function
    fetch_and_load_task = extract_load_yahoo_finance(
        tickers=TICKERS_TO_FETCH,
        conn_id=SF_CONN,
        db=SF_DB,
        schema=SF_SCHEMA,
        table=YFINANCE_TABLE,
        logical_date="{{ ds }}",  # Pass logical_date using Airflow's macro
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Define dependencies
    (
        start
        >> ensure_schema_exist
        >> ensure_table_exist
        >> fetch_and_load_task
        >> end
    )


# Instantiate the DAG
yahoo_finance_pipeline()