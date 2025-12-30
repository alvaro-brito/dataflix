FROM python:3.11-slim

WORKDIR /usr/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and dependencies
RUN pip install --no-cache-dir \
    dbt-core==1.7.0 \
    dbt-clickhouse==1.7.0 \
    flask==3.0.0 \
    psycopg2-binary==2.9.9 \
    clickhouse-driver==0.2.6 \
    pandas==2.1.3 \
    "protobuf<5.0"

EXPOSE 5000

CMD ["python", "webhook_server.py"]
