import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.yfinance_loader import (
    fetch_and_load_stock_data, 
    YahooFinanceFetcher, 
    SnowflakeConnectionFactory
 )

class TestYahooFinanceFetcher(unittest.TestCase):
    @patch("yfinance.Ticker")
    def test_fetch_data_success(self, mock_ticker):
        mock_history = MagicMock()
        mock_history.history.return_value = pd.DataFrame({"Date": ["2025-04-15"], "Close": [150]})
        mock_ticker.return_value = mock_history

        fetcher = YahooFinanceFetcher()
        result = fetcher.fetch_data("AAPL", "2025-04-01", "2025-04-15")

        self.assertIsNotNone(result)
        self.assertIn("Close", result.columns)

    @patch("yfinance.Ticker")
    def test_fetch_data_no_data(self, mock_ticker):
        mock_history = MagicMock()
        mock_history.history.return_value = pd.DataFrame()
        mock_ticker.return_value = mock_history

        fetcher = YahooFinanceFetcher()
        result = fetcher.fetch_data("AAPL", "2025-04-01", "2025-04-15")

        self.assertIsNone(result)

class TestSnowflakeConnectionFactory(unittest.TestCase):
    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_create_connection(self, mock_hook):
        mock_conn = MagicMock()
        mock_hook.return_value.get_conn.return_value = mock_conn

        conn = SnowflakeConnectionFactory.create_connection("mock_conn_id")
        self.assertEqual(conn, mock_conn)

if __name__ == "__main__":
    unittest.main()