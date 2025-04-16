import yfinance as yf
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from snowflake.connector.pandas_tools import write_pandas
from pendulum import now
import logging
from abc import ABC, abstractmethod

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Add a file handler to log to a file
file_handler = logging.FileHandler("yfinance_loader.log")
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

class DataFetcherStrategy(ABC):
    @abstractmethod
    def fetch_data(self, ticker_symbol, start_date_str, end_date_str):
        pass

class YahooFinanceFetcher(DataFetcherStrategy):
    def fetch_data(self, ticker_symbol, start_date_str, end_date_str):
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(start=start_date_str, end=end_date_str)
        if hist.empty:
            log.warning(f"No data returned for ticker: {ticker_symbol}")
            return None
        return hist

class SnowflakeConnectionFactory:
    @staticmethod
    def create_connection(snowflake_conn_id):
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        return hook.get_conn()

def write_snowflake(all_data, snowflake_conn_id, database, schema, table_name, chunk_size):
    # Combine dataframes
    combined_df = pd.concat(all_data, ignore_index=True)
    log.info(f"Combined DataFrame shape: {combined_df.shape}")

    # Ensure column names match Snowflake table (case-insensitive by default with write_pandas)
    # Quote column names with spaces to avoid SQL syntax errors
    combined_df.columns = combined_df.columns.str.replace(" ", "_").str.upper()

    log.info(
        f"Attempting to load {combined_df.shape[0]} rows into Snowflake table {database}.{schema}.{table_name}"
    )
    print(combined_df.columns)

    conn = None  # Initialize conn to avoid unbound variable error
    try:
        # Get Snowflake connection using factory
        conn = SnowflakeConnectionFactory.create_connection(snowflake_conn_id)

        # Use write_pandas for efficient bulk loading from DataFrame
        # Note: This performs individual INSERT statements in batches behind the scenes,
        # it's NOT using COPY INTO. For very large volumes, staging + COPY INTO is faster.
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=combined_df,
            table_name=table_name.upper(),  # write_pandas often expects uppercase
            schema=schema.upper(),
            database=database.upper(),
            chunk_size=chunk_size,
            use_logical_type=True,  # Ensure proper handling of datetime with timezone
        )

        if success:
            log.info(f"Successfully loaded {nrows} rows in {nchunks} chunks.")
        else:
            # This part might not be reached if write_pandas raises an exception on failure
            log.error("Snowflake write_pandas reported failure.")
            raise AirflowException("Snowflake write_pandas failed.")

    except Exception as e:
        log.error(f"Error loading data into Snowflake: {e}")
        raise AirflowException(f"Snowflake loading error: {e}")
    finally:
        # Ensure connection is closed
        if "conn" in locals() and conn is not None:
            conn.close()
            log.info("Snowflake connection closed.")    


def fetch_and_load_stock_data(
    tickers: list[str],
    snowflake_conn_id: str,
    table_name: str,
    schema: str,
    database: str,
    start_date_str: str,
    end_date_str: str,
    chunk_size: int = 10000,
    fetcher_strategy: DataFetcherStrategy = YahooFinanceFetcher(),
):
    """
    Fetches historical stock data for given tickers using a data fetching strategy and loads it
    directly into a specified Snowflake table using SnowflakeHook and write_pandas.

    Args:
        tickers (list[str]): List of stock tickers (e.g., ['MSFT', 'AAPL']).
        snowflake_conn_id (str): Airflow connection ID for Snowflake.
        table_name (str): Target table name in Snowflake.
        schema (str): Target schema name in Snowflake.
        database (str): Target database name in Snowflake.
        start_date_str (str): Start date for historical data ('YYYY-MM-DD').
        end_date_str (str): End date for historical data ('YYYY-MM-DD').
        chunk_size (int): Number of rows to write per chunk in write_pandas.
        fetcher_strategy (DataFetcherStrategy): Strategy for fetching data.
    """
    all_data = []
    load_timestamp = now("UTC").to_iso8601_string()
    log.info(f"Load timestamp: {load_timestamp}")

    log.info(
        f"Fetching data for tickers: {tickers} from {start_date_str} to {end_date_str}"
    )

    for ticker_symbol in tickers:
        try:
            hist = fetcher_strategy.fetch_data(ticker_symbol, start_date_str, end_date_str)
            if hist is None:
                continue

            hist["TICKER"] = ticker_symbol
            hist["LOADTIMESTAMP"] = load_timestamp
            hist.reset_index(inplace=True)
            all_data.append(hist)
            log.info(f"Successfully fetched data for {ticker_symbol}")

        except Exception as e:
            log.error(f"Failed to fetch data for ticker {ticker_symbol}: {e}")

            if all_data:
                log.info('Storing into Snowflake ...')
                write_snowflake(all_data=all_data, 
                                snowflake_conn_id=snowflake_conn_id,
                                database=database,
                                schema=schema,
                                table_name=table_name,
                                chunk_size=chunk_size
                                )
                all_data.clear()

    if not all_data:
        log.warning("No data fetched for any ticker. Skipping Snowflake load.")
        return

    write_snowflake(all_data=all_data, 
                    snowflake_conn_id=snowflake_conn_id,
                    database=database,
                    schema=schema,
                    table_name=table_name,
                    chunk_size=chunk_size
                    )
