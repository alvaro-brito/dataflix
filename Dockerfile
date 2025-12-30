FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    flask==3.0.0 \
    flask-cors==4.0.0 \
    flask-sqlalchemy==3.1.1 \
    psycopg2-binary==2.9.9 \
    mlflow==2.10.2 \
    scikit-learn==1.3.2 \
    pandas==2.1.3 \
    numpy==1.26.2 \
    requests==2.31.0 \
    boto3==1.29.7

COPY requirements.txt .
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

EXPOSE 5002

CMD ["python", "app.py"]
