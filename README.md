# Airflow Stock Data Pipeline

This project is an Airflow-based pipeline designed to fetch historical stock data using the `yfinance` library and load it into a Snowflake database. The pipeline is containerized using Docker and leverages the Astronomer runtime for Airflow.

## Features

- Fetches historical stock data for a configurable list of tickers.
- Loads the data into a Snowflake table using the `write_pandas` method for efficient bulk loading.
- Fully containerized setup for easy deployment.

## Project Structure

```
├── Dockerfile                # Docker configuration for the Airflow environment
├── packages.txt              # (Optional) Additional system packages (empty in this case)
├── requirements.txt          # Python dependencies for the project
├── dags/
│   └── dag_yfinance_load.py  # Airflow DAG definition
├── src/
│   └── yfinance_loader.py    # Logic for fetching and loading stock data
```

## Prerequisites

- Docker and Docker Compose installed on your system.
- A Snowflake account with a configured connection in Airflow.

## Setup and Usage

1. **Clone the Repository**

   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. **Build the Docker Image**

   ```bash
   docker build -t airflow-stock-pipeline .
   ```

3. **Run the Airflow Environment**

   Use Docker Compose to start the Airflow environment:

   ```bash
   docker-compose up
   ```

4. **Configure Airflow Connections**

   - Add a Snowflake connection in the Airflow UI with the ID `SF_CONN`.

5. **Trigger the DAG**

   - Access the Airflow UI at `http://localhost:8080`.
   - Enable and trigger the `dag_yfinance_load` DAG.

## Configuration

The following configurations can be modified in `dags/dag_yfinance_load.py`:

- `SF_CONN`: Airflow connection ID for Snowflake.
- `SF_DB`, `SF_SCHEMA`, `YFINANCE_TABLE`: Snowflake database, schema, and table names.
- `TICKERS_TO_FETCH`: List of stock tickers to fetch data for.

## Dependencies

The project uses the following Python libraries:

- `pandas`
- `yfinance`
- `snowflake-connector-python`
- `apache-airflow-providers-snowflake`
- `apache-airflow-providers-common-sql`

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgments

- [Astronomer](https://www.astronomer.io/) for the Airflow runtime.
- [Yahoo Finance](https://finance.yahoo.com/) for stock data.
