# Use a specific base image for Airflow
FROM quay.io/astronomer/astro-runtime:12.8.0

# Set the working directory
WORKDIR /usr/local/airflow

# Copy and install dependencies in one step to reduce layers
COPY requirements.txt ./
RUN python -m virtualenv airflow_env \
    && source airflow_env/bin/activate \
    && pip install --no-cache-dir -r requirements.txt \
    && deactivate
