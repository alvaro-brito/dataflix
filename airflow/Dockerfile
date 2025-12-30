FROM apache/airflow:2.7.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    clickhouse-driver==0.2.6 \
    pandas==2.1.3 \
    numpy==1.26.2 \
    requests==2.31.0

# Copy requirements.txt if it exists
COPY requirements.txt .
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

EXPOSE 8080
