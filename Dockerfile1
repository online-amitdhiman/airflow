FROM quay.io/astronomer/astro-runtime:12.8.0

WORKDIR /usr/local/airflow
COPY requirements.txt ./
RUN python -m virtualenv airflow_env && source airflow_env/bin/activate && \
    pip install --no-cache-dir -r requirements.txt && deactivate
    