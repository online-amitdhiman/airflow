# Astronomer Snowflake Integration POC

This project is a Proof of Concept (POC) for integrating Astronomer with Snowflake. It demonstrates how to fetch historical stock data using the `yfinance` library and load it into a Snowflake database. The pipeline is containerized and leverages the Astronomer runtime for orchestration.

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
│   └── dag_yfinance_load.py  # DAG definition
├── src/
│   └── yfinance_loader.py    # Logic for fetching and loading stock data
```

## Prerequisites

- Astronomer account and access to the Astronomer web UI.
- A Snowflake account with a private key configured for authentication.

## Setup and Usage

1. **Clone the Repository**

   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. **Connect the Repository to Astronomer**

   - Log in to the Astronomer web UI.
   - Navigate to your workspace and create a new deployment.
   - Connect your GitHub repository to the deployment by following the on-screen instructions.

3. **Generate Private/Public Key for Snowflake Connection**

   - Generate a private RSA key using OpenSSL:
     ```bash
     openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
     ```
   - Generate the associated public key:
     ```bash
     openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
     ```
   - In the Snowflake UI, create a new user or update an existing user to include the public key:
     ```sql
     ALTER USER <your_user> SET RSA_PUBLIC_KEY = '<your_public_key>';
     ```

4. **Steps to Create Snowflake Connection using Private Key**

   1. **Log in to the Astronomer Web UI**
      - Navigate to your Astronomer workspace and select the deployment where you want to configure the Snowflake connection.

   2. **Access the Connections Section**
      - In the Astronomer Web UI, go to the `Environment Variables` or `Connections` section for your deployment.

   3. **Create a New Connection**
      - Click on `Add Connection` or a similar button to create a new connection.
      - Select `Snowflake` as the connection type.

   4. **Fill in the Connection Details**
      - **Connection ID**: Enter a unique name for the connection, e.g., `SF_CONN`.
      - **Account**: Provide your Snowflake account identifier (e.g., `abc12345.us-east-1`).
      - **Username**: Enter your Snowflake username.
      - **Private Key**: Paste the content of your private key. Ensure the private key is formatted correctly (replace newlines with `\n`).
      - **Database**: Specify the default database for the connection.
      - **Schema**: Specify the default schema for the connection.
      - **Warehouse**: Enter the name of the Snowflake warehouse to use.
      - **Role**: Provide the role to be used for the connection.

   5. **Save the Connection**
      - Click `Save` to store the connection. The connection will now be available for use in your DAGs.

   6. **Test the Connection**
      - Optionally, test the connection to ensure it is configured correctly.

5. **Deploy the DAG**

   - Push your changes to the connected GitHub repository.
   - Astronomer will automatically detect the changes and deploy the updated DAGs to your environment.

6. **Trigger the DAG**

   - Access the Astronomer web UI.
   - Navigate to the Airflow UI for your deployment.
   - Enable and trigger the `dag_yfinance_load` DAG.

7. **Monitor and Debug**

   - Use the Astronomer web UI to monitor DAG runs and view logs for debugging.

## Configuration

The following configurations can be modified in `dags/dag_yfinance_load.py`:

- `SF_CONN`: Astronomer connection ID for Snowflake.
- `SF_DB`, `SF_SCHEMA`, `YFINANCE_TABLE`: Snowflake database, schema, and table names.
- `TICKERS_TO_FETCH`: List of stock tickers to fetch data for.

## GitHub Integration

1. **Connect GitHub Repository**
   - In the Astronomer Web UI, navigate to the `Deployments` section.
   - Select your deployment and go to the `Source Control` or `GitHub Integration` tab.
   - Follow the on-screen instructions to connect your GitHub repository.

2. **Push Changes to GitHub**
   - Ensure your DAGs and other project files are pushed to the connected GitHub repository.

3. **Automatic Deployment**
   - Astronomer will automatically detect changes in the GitHub repository and deploy the updated DAGs to your environment.

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

- [Astronomer](https://www.astronomer.io/) for the orchestration runtime.
- [Yahoo Finance](https://finance.yahoo.com/) for stock data.
