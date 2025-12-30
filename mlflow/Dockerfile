FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow and dependencies
RUN pip install --no-cache-dir \
    mlflow==2.10.2 \
    boto3==1.29.7 \
    pymysql==1.1.0 \
    cryptography==42.0.0 \
    scikit-learn==1.3.2 \
    pandas==2.1.3 \
    numpy==1.26.2 \
    clickhouse-driver==0.2.6 \
    flask==3.0.0

# Copy scripts
COPY train_model.py /app/train_model.py
COPY training_server.py /app/training_server.py
COPY start_services.sh /app/start_services.sh
RUN chmod +x /app/start_services.sh

EXPOSE 5000 5001

CMD ["/app/start_services.sh"]
