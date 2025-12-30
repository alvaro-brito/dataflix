# Dataflix - Movie Recommendation System with Collaborative Filtering

A complete and functional movie recommendation system using **Collaborative Filtering** with **MLflow**, built with a modern Data Stack architecture.

## 🎯 Overview

Dataflix is a reference project demonstrating how to build a production-grade movie recommendation system, integrating:

- **PostgreSQL**: Transactional database (OLTP)
- **Airflow**: ETL pipeline orchestration
- **ClickHouse**: Data Warehouse for analytics
- **dbt**: Data transformations
- **MLflow**: Model training and versioning
- **Minio**: Artifact storage (S3-compatible)
- **Apache Superset**: Data visualization and dashboards
- **Backend API**: Flask for serving recommendations
- **CLI**: Command-line interface for interaction

## 📊 Architecture

```
PostgreSQL (OLTP)
    ↓
Airflow (ETL with data masking)
    ↓
ClickHouse Raw (Masked raw data)
    ↓
dbt (Transformations)
    ↓
ClickHouse Analytics (Analytical data)
    ↓
MLflow (Model training) + Superset (Visualization)
    ↓
Backend API + CLI (Recommendations)
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ (for local CLI)
- Minimum 4GB RAM available

### Installation

1. **Clone or navigate to the project directory**:

```bash
cd dataflix
```

2. **Start all services**:

```bash
./run.sh
```

This script will:
- Create necessary directories
- Build Docker images
- Start all services
- Display endpoints and credentials

3. **Check service status**:

```bash
python3 check_all_works.py
```

4. **Configure Superset with dashboards** (optional):

```bash
python3 scripts/setup_superset_dataflix.py
```

This script automatically creates:
- Connection to ClickHouse
- Datasets from analytical tables
- Dashboard with statistical charts

5. **Access services**:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| ClickHouse | http://localhost:8123 | default / clickhouse123 |
| MLflow | http://localhost:5003 | - |
| Minio Console | http://localhost:9003 | minio / minio123 |
| Backend API | http://localhost:5002 | - |
| Superset | http://localhost:8088 | admin / admin |

## 📝 Data Flow

### 1. Ingestion (PostgreSQL)

Data is inserted into PostgreSQL via CLI or Backend API:
- Users (name, city, state, etc.)
- Movies (title, genre, director, etc.)
- Watched movies
- Ratings/Likes

### 2. Extraction (Airflow)

The `elt_pipeline` DAG runs daily:
- Extracts data from PostgreSQL
- **Masks sensitive data** (emails, names)
- Loads into ClickHouse (raw layer)
- Triggers webhook for dbt

### 3. Transformation (dbt)

Webhook server executes dbt transformations:
- Staging: Cleaning and preparation
- Marts: Final analytical tables
- User-movie matrix for collaborative filtering

### 4. Training (MLflow)

Training script:
- Loads matrix from ClickHouse
- Trains NMF (Non-negative Matrix Factorization) model
- Registers model and metrics in MLflow
- Saves artifacts to Minio

### 5. Visualization (Superset)

Interactive dashboard with:
- Statistical charts about movies and users
- Analysis of ratings and interactions
- Interactive filters and drill-down

### 6. Recommendation (Backend + CLI)

Backend loads model and generates recommendations:
- Filters unwatch movies
- Calculates similarity scores
- Returns top-N recommendations

## 🎨 Superset Dashboards

The project includes an automatic setup script that creates a dashboard with the following charts:

### Available Charts

- **Movies by Genre**: Pie chart showing distribution of movies by genre
- **Average Rating per Movie**: Bar chart with average movie ratings
- **Total Watched Movies**: Big number metric showing total views
- **Users by State**: Distribution of users by state
- **User-Movie Interaction Matrix**: Heatmap showing interactions between users and movies
- **Aggregated Ratings**: Table with rating statistics

### How to Use Superset

1. After starting services with `./run.sh`
2. Run the setup script:
   ```bash
   python3 scripts/setup_superset_dataflix.py
   ```
3. Access http://localhost:8088
4. Log in with admin / admin
5. Navigate to "Dashboards" and select "Dataflix Analytics"

## 📊 Main Components

### PostgreSQL (5432)

**Tables**:
- `users`: User information
- `movies`: Movie catalog
- `watched_movies`: Watched movies
- `ratings`: Ratings and likes

### ClickHouse (8123 HTTP, 9000 TCP)

**Databases**:
- `raw`: Raw data (with masking)
- `analytics`: Transformed data

**Tables**:
- `raw.users`, `raw.movies`, `raw.watched_movies`, `raw.ratings`
- `analytics.users`, `analytics.movies`, `analytics.user_movie_matrix`, `analytics.ratings_aggregated`

### Airflow (8080)

**DAG**: `elt_pipeline`
- Runs daily
- Extracts data from PostgreSQL
- Masks sensitive data
- Loads into ClickHouse
- Triggers dbt transformations

### dbt (5001)

**Models**:
- `stg_users`, `stg_movies`, `stg_watched_movies`, `stg_ratings` (staging)
- `mart_users`, `mart_movies`, `mart_user_movie_matrix`, `mart_ratings_aggregated` (marts)

### MLflow (5003)

**Experiment**: `dataflix-collaborative-filtering`
- Algorithm: NMF (Non-negative Matrix Factorization)
- Parameters: n_components=10, max_iter=200
- Metrics: RMSE, MAE, Sparsity

### Apache Superset (8088)

**Data Visualization**: Interactive dashboard with statistical charts

**Features**:
- Connection to ClickHouse
- Datasets from analytical tables
- Interactive charts (pie, bar, heatmap, tables)
- Responsive dashboard
- Filters and drill-down

### Backend API (5002)

**Endpoints**:
- `GET /users` - List users
- `POST /users` - Create user
- `GET /movies` - List movies
- `POST /watched` - Mark as watched
- `POST /ratings` - Submit rating
- `GET /recommendations/{user_id}` - Get recommendations

### CLI (Python)

**Features**:
- Select/create user
- Browse and filter movies
- Mark movies as watched
- Submit ratings/likes
- View recommendations

## 📚 CLI Usage

```bash
# Install dependencies
pip install -r cli/requirements.txt

# Run CLI
python3 cli/main.py
```

**Main Menu**:
1. Select/Create User
2. Browse Movies
3. View Recommendations
4. View Watched Movies
5. View My Ratings

## 🔐 Security and Data Masking

### Masked Data in ETL

The Airflow pipeline masks sensitive data:

- **Emails**: Converted to SHA256 hash (first 16 chars)
- **First Names**: Masked (e.g., "John" → "J***")
- **Last Names**: Masked (e.g., "Doe" → "D**")

Public data (city, state, country) is preserved for analysis.

### Masking Example

```python
# Original Email: john@example.com
# Masked Email: 8d969eef6ecad3c29a3a873fba8f4f78

# Original Name: John
# Masked Name: J***

# Original Last Name: Doe
# Masked Last Name: D**
```

## 📊 Sample Data

The project includes seed data with:
- 8 users
- 20 movies
- 33 watched movies
- 33 ratings

## 🧪 Tests

### Automatic Verification

```bash
python3 check_all_works.py
```

Checks:
- Connectivity with PostgreSQL
- Connectivity with ClickHouse
- Status of all services
- DAG presence in Airflow
- Backend API endpoints
- Superset availability

### Manual Tests

1. **Airflow**: Access http://localhost:8080 and check `elt_pipeline` DAG
2. **ClickHouse**: Query data in `raw` and `analytics`
3. **MLflow**: Access http://localhost:5003 and check experiment
4. **Superset**: Access http://localhost:8088 and view dashboards
5. **Backend**: Test endpoints with curl or Postman
6. **CLI**: Run `python3 cli/main.py` and navigate menus

## 📈 Model Training

To train the model manually:

```bash
# Inside MLflow container
docker exec mlflow-server-dataflix python /app/train_model.py

# Or locally (if dependencies are present)
python3 mlflow/train_model.py
```

## 🛑 Stop Services

```bash
./stop.sh
```

Options:
- Stop containers
- Remove volumes (optional)

## 📋 Project Structure

```
dataflix/
├── .env                          # Environment variables
├── docker-compose.yml            # Docker configuration
├── run.sh                        # Startup script
├── stop.sh                       # Shutdown script
├── check_all_works.py            # Functionality check
├── ARCHITECTURE.md               # Architecture documentation
├── README.md                     # This file
├── postgres/
│   └── init/
│       ├── 01-init-schema.sql    # PostgreSQL schema
│       └── 02-seed-data.sql      # Seed data
├── clickhouse/
│   └── init/
│       ├── 01-init-databases.sql # ClickHouse databases
│       └── 02-init-tables.sql    # ClickHouse tables
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── elt_pipeline.py       # Main DAG
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── webhook_server.py         # Webhook for dbt
│   └── models/
│       ├── staging/              # Staging models
│       └── marts/                # Marts models
├── mlflow/
│   ├── Dockerfile
│   ├── start_services.sh     # Startup script
│   ├── train_model.py        # Training script
│   └── training_server.py    # Training API server
├── superset/
│   └── config/
│       └── superset_config.py    # Superset configuration
├── scripts/
│   └── setup_superset_dataflix.py # Automatic Superset setup
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                    # Flask API
└── cli/
    ├── requirements.txt
    └── main.py                   # Python CLI
```

## 🐛 Troubleshooting

### Services do not start

```bash
# Check logs
docker-compose logs -f [service]

# Example
docker-compose logs -f postgres-source
```

### Airflow cannot find DAG

```bash
# Check permissions
chmod -R 777 airflow/dags
chmod -R 777 airflow/logs

# Restart Airflow
docker-compose restart airflow-webserver airflow-scheduler
```

### ClickHouse does not connect

```bash
# Check status
docker-compose logs clickhouse-server

# Check port
netstat -an | grep 9000
```

### Backend does not connect to PostgreSQL

```bash
# Check environment variables
docker-compose exec backend env | grep POSTGRES

# Check connectivity
docker-compose exec backend ping postgres-source
```

### Superset does not load data from ClickHouse

```bash
# Check if ClickHouse is running
docker-compose logs clickhouse-server

# Check connection manually
docker-compose exec superset python -c "from clickhouse_driver import Client; c = Client('clickhouse-server'); print(c.execute('SELECT 1'))"
```

## 📚 References

- [Apache Airflow](https://airflow.apache.org/)
- [ClickHouse](https://clickhouse.com/)
- [dbt](https://www.getdbt.com/)
- [MLflow](https://mlflow.org/)
- [Minio](https://min.io/)
- [Apache Superset](https://superset.apache.org/)
- [Flask](https://flask.palletsprojects.com/)

## 📝 Notes

- This is a reference project for educational purposes
- Do not use in production without security adjustments
- Seed data included for testing
- Model trained with small dataset (8 users, 20 movies)

## 📝 TODO

- [x] Superset Error - Metrics
- [x] Superset Error - Charts
- [x] Superset Error - Dashboard
