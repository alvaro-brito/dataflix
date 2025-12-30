#!/bin/bash

echo "=========================================="
echo "Starting MLflow Services"
echo "=========================================="

# Iniciar MLflow server em background
echo "Starting MLflow Tracking Server on port 5000..."
mlflow server \
    --backend-store-uri "${MLFLOW_BACKEND_STORE_URI}" \
    --default-artifact-root "${MLFLOW_DEFAULT_ARTIFACT_ROOT}" \
    --host 0.0.0.0 \
    --port 5000 &

# Wait for MLflow server to start
echo "Waiting for MLflow server to start..."
sleep 10

# Start Training API server
echo "Starting Training API Server on port 5001..."
python /app/training_server.py
