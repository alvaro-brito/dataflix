# Dataflix - Movie Recommendation System with Collaborative Filtering

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATAFLIX ARCHITECTURE                    │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────────┐
│   PostgreSQL 15      │ (OLTP - Source)
│  - Users             │
│  - Movies            │
│  - Watched Movies    │
│  - Movie Ratings     │
└──────────┬───────────┘
           │
           │ Extract
           ▼
┌──────────────────────┐
│  Airflow DAGs        │ (Orchestration)
│  - ELT Pipeline      │
│  - Data Masking      │
│  - Raw Data Loading  │
└──────────┬───────────┘
           │
           │ Load (Raw)
           ▼
┌──────────────────────┐
│  ClickHouse Raw      │ (Data Warehouse - Raw Layer)
│  - raw.users         │
│  - raw.movies        │
│  - raw.watched_movies│
│  - raw.ratings       │
└──────────┬───────────┘
           │
           │ Transform (dbt + Webhook)
           ▼
┌──────────────────────┐
│ ClickHouse Analytics │ (Data Warehouse - Analytics Layer)
│  - analytics.users   │
│  - analytics.movies  │
│  - analytics.user_   │
│    movie_matrix      │
│  - analytics.ratings │
└──────────┬───────────┘
           │
           │ Train Model
           ▼
┌──────────────────────┐
│  MLflow + Minio      │ (Model Registry & Artifacts)
│  - Trained Models    │
│  - Model Versions    │
│  - Metrics & Params  │
└──────────┬───────────┘
           │
           │ Load Model
           ▼
┌──────────────────────┐
│ Backend API (Flask)  │ (API Layer)
│ - User Management    │
│ - Movie Management   │
│ - Recommendations    │
└──────────┬───────────┘
           │
           │ Interact
           ▼
┌──────────────────────┐
│  CLI (Python)        │ (User Interface)
│ - Select User        │
│ - Browse Movies      │
│ - Rate Movies        │
│ - Get Recommendations│
└──────────────────────┘
```

## Components

### 1. PostgreSQL (Source Database)
- **Port**: 5432
- **User**: dataflix
- **Password**: dataflix123
- **Database**: dataflix_db

**Tables**:
- `users`: User information (name, city, state, etc.)
- `movies`: Movie catalog
- `watched_movies`: Movies watched by user
- `ratings`: Likes/ratings of movies by user

### 2. ClickHouse (Data Warehouse)
- **HTTP Port**: 8123
- **TCP Port**: 9000
- **User**: default
- **Password**: clickhouse123

**Databases**:
- `raw`: Raw data from PostgreSQL (with sensitive data masking)
- `analytics`: Transformed data ready for analysis

### 3. Airflow (Orchestration)
- **Port**: 8080
- **User**: admin
- **Password**: admin
- **DAG**: `elt_pipeline` - Runs daily

**Features**:
- Extracts data from PostgreSQL
- Applies sensitive data masking (emails, names)
- Loads into ClickHouse (raw layer)
- Triggers webhook for dbt

### 4. dbt (Transformations)
- **Port**: 5001
- **Webhook Server**: Receives Airflow events
- **Models**:
  - Staging: Data cleaning and preparation
  - Marts: Final analytical tables

### 5. MLflow + Minio (Model Registry)
- **MLflow Port**: 5003
- **Minio Port**: 9002 (API), 9003 (Console)
- **Model**: Collaborative Filtering (Matrix Factorization)

### 6. Backend API (Flask)
- **Port**: 5002
- **Endpoints**:
  - POST `/users` - Create user
  - GET `/users/{id}` - Get user
  - GET `/movies` - List movies
  - POST `/watched` - Record watched movie
  - POST `/ratings` - Record rating
  - GET `/recommendations/{user_id}` - Get recommendations

### 7. CLI (Python)
- **Interaction**: Command line
- **Features**:
  - Select/create user
  - Search and filter movies
  - Record watched movies
  - Record ratings
  - View recommendations

## Data Flow

1. **Ingestion**: Data is inserted into PostgreSQL via CLI/Backend
2. **Extraction**: Airflow extracts data daily
3. **Masking**: Sensitive data is masked
4. **Loading**: Data loaded into ClickHouse (raw)
5. **Transformation**: dbt transforms data for analytics
6. **Training**: MLflow trains collaborative filtering model
7. **Recommendation**: Backend loads model and generates recommendations
8. **Visualization**: CLI displays recommendations to user

## Environment Variables

```env
# PostgreSQL
POSTGRES_USER=dataflix
POSTGRES_PASSWORD=dataflix123
POSTGRES_DB=dataflix_db

# ClickHouse
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse123

# Airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow

# MLflow
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
MLFLOW_S3_ENDPOINT_URL=http://minio:9000

# Backend
FLASK_ENV=development
FLASK_DEBUG=1
```

## Technologies

- **Database**: PostgreSQL 15, ClickHouse
- **Orchestration**: Apache Airflow
- **Transformation**: dbt
- **ML**: MLflow, scikit-learn
- **Storage**: Minio (S3-compatible)
- **API**: Flask
- **CLI**: Click/Typer
- **Containerization**: Docker, Docker Compose

## How to Run

```bash
# Start all services
./run.sh

# Check status
python3 check_all_works.py

# Stop all services
./stop.sh
```

