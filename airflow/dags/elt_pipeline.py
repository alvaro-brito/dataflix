"""
DAG: ELT Pipeline - PostgreSQL → ClickHouse → dbt → MLflow
Description: Extracts data from PostgreSQL, loads into ClickHouse with sensitive data masking,
executes dbt transformations, and trains collaborative filtering model with MLflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import psycopg2
from clickhouse_driver import Client
import logging
import hashlib
import os
import requests
import json

# Logger
logger = logging.getLogger(__name__)

# Configurations
POSTGRES_CONN = {
    'host': os.getenv('POSTGRES_HOST', 'postgres-source'),
    'port': 5432,
    'user': os.getenv('POSTGRES_USER', 'dataflix'),
    'password': os.getenv('POSTGRES_PASSWORD', 'dataflix123'),
    'database': os.getenv('POSTGRES_DB', 'dataflix_db')
}

CLICKHOUSE_CONN = {
    'host': os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'),
    'port': 9000,
    'user': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse123')
}

MLFLOW_URL = os.getenv('MLFLOW_URL', 'http://mlflow-server:5000')

# Default arguments
default_args = {
    'owner': 'dataflix-pipeline',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG
dag = DAG(
    'elt_pipeline',
    default_args=default_args,
    description='ELT Pipeline: PostgreSQL → ClickHouse → dbt → MLflow',
    schedule_interval='@daily',
    catchup=False,
    tags=['elt', 'dataflix', 'clickhouse', 'mlflow'],
)

def mask_email(email: str) -> str:
    """Mask email with hash"""
    return hashlib.sha256(email.encode()).hexdigest()[:16]

def mask_name(name: str) -> str:
    """Mask name keeping first letter"""
    if not name or len(name) < 2:
        return "***"
    return name[0] + "*" * (len(name) - 1)

def create_clickhouse_databases():
    """Create databases in ClickHouse"""
    logger.info("Creating databases in ClickHouse...")
    try:
        client = Client(
            CLICKHOUSE_CONN['host'],
            port=CLICKHOUSE_CONN['port'],
            user=CLICKHOUSE_CONN['user'],
            password=CLICKHOUSE_CONN['password']
        )
        client.execute("CREATE DATABASE IF NOT EXISTS raw")
        client.execute("CREATE DATABASE IF NOT EXISTS analytics")
        logger.info("✓ Databases created successfully")
    except Exception as e:
        logger.error(f"Error creating databases: {str(e)}")
        raise

def extract_and_load_users():
    """Extract users from PostgreSQL and load into ClickHouse with masking"""
    logger.info("Starting ELT for table: users")
    
    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**POSTGRES_CONN)
        pg_cursor = pg_conn.cursor()
        
        # Fetch data (excluding updated_at which doesn't exist in ClickHouse)
        pg_cursor.execute("""
            SELECT user_id, username, email, first_name, last_name,
                   city, state, country, age, created_at
            FROM users ORDER BY user_id
        """)
        columns = [desc[0] for desc in pg_cursor.description]
        rows = pg_cursor.fetchall()
        
        logger.info(f"Extracted {len(rows)} users")
        
        # Connect to ClickHouse
        ch_client = Client(
            CLICKHOUSE_CONN['host'],
            port=CLICKHOUSE_CONN['port'],
            user=CLICKHOUSE_CONN['user'],
            password=CLICKHOUSE_CONN['password']
        )
        
        # Recreate table (drop + create to ensure schema update)
        ch_client.execute("DROP TABLE IF EXISTS raw.users")
        ch_client.execute("""
            CREATE TABLE raw.users (
                user_id Int32,
                username String,
                email String,
                first_name String,
                last_name String,
                city String,
                state String,
                country String,
                age Int32,
                created_at DateTime
            ) ENGINE = MergeTree() ORDER BY user_id
        """)
        
        # Mask sensitive data
        masked_rows = []
        for row in rows:
            masked_row = list(row)
            # Mask email (index 2)
            if len(masked_row) > 2 and masked_row[2]:
                masked_row[2] = mask_email(masked_row[2])
            # Mask first name (index 3)
            if len(masked_row) > 3 and masked_row[3]:
                masked_row[3] = mask_name(masked_row[3])
            # Mask last name (index 4)
            if len(masked_row) > 4 and masked_row[4]:
                masked_row[4] = mask_name(masked_row[4])
            masked_rows.append(tuple(masked_row))
        
        # Insert data
        if masked_rows:
            ch_client.execute(
                "INSERT INTO raw.users VALUES",
                masked_rows
            )
            logger.info(f"Loaded {len(masked_rows)} users into raw.users (with masking)")
        
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("✓ ELT completed for table: users")
        
    except Exception as e:
        logger.error(f"Error processing users: {str(e)}")
        raise

def extract_and_load_table(table_name: str):
    """Extract generic data from PostgreSQL and load into ClickHouse"""
    logger.info(f"Starting ELT for table: {table_name}")
    
    try:
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**POSTGRES_CONN)
        pg_cursor = pg_conn.cursor()
        
        # Fetch data with specific columns for each table
        if table_name == 'movies':
            pg_cursor.execute("""
                SELECT movie_id, title, description, genre, release_year,
                       director, duration_minutes, imdb_rating, created_at
                FROM movies ORDER BY movie_id
            """)
        elif table_name == 'watched_movies':
            pg_cursor.execute("""
                SELECT watched_id, user_id, movie_id, watched_at
                FROM watched_movies ORDER BY watched_id
            """)
        elif table_name == 'ratings':
            pg_cursor.execute("""
                SELECT rating_id, user_id, movie_id, rating, liked, rated_at
                FROM ratings ORDER BY rating_id
            """)
        else:
            pg_cursor.execute(f"SELECT * FROM {table_name} ORDER BY 1")

        columns = [desc[0] for desc in pg_cursor.description]
        rows = pg_cursor.fetchall()
        
        logger.info(f"Extracted {len(rows)} records from {table_name}")
        
        # Connect to ClickHouse
        ch_client = Client(
            CLICKHOUSE_CONN['host'],
            port=CLICKHOUSE_CONN['port'],
            user=CLICKHOUSE_CONN['user'],
            password=CLICKHOUSE_CONN['password']
        )
        
        # Recreate specific tables (drop + create to ensure schema update)
        ch_client.execute(f"DROP TABLE IF EXISTS raw.{table_name}")

        if table_name == 'movies':
            ch_client.execute(f"""
                CREATE TABLE raw.{table_name} (
                    movie_id Int32,
                    title String,
                    description String,
                    genre String,
                    release_year Int32,
                    director String,
                    duration_minutes Int32,
                    imdb_rating Float64,
                    created_at DateTime
                ) ENGINE = MergeTree() ORDER BY movie_id
            """)
        elif table_name == 'watched_movies':
            ch_client.execute(f"""
                CREATE TABLE raw.{table_name} (
                    watched_id Int32,
                    user_id Int32,
                    movie_id Int32,
                    watched_at DateTime
                ) ENGINE = MergeTree() ORDER BY (user_id, movie_id)
            """)
        elif table_name == 'ratings':
            ch_client.execute(f"""
                CREATE TABLE raw.{table_name} (
                    rating_id Int32,
                    user_id Int32,
                    movie_id Int32,
                    rating Float64,
                    liked UInt8,
                    rated_at DateTime
                ) ENGINE = MergeTree() ORDER BY (user_id, movie_id)
            """)
        
        # Insert data
        if rows:
            ch_client.execute(
                f"INSERT INTO raw.{table_name} VALUES",
                rows
            )
            logger.info(f"Loaded {len(rows)} records into raw.{table_name}")
        
        pg_cursor.close()
        pg_conn.close()
        
        logger.info(f"✓ ELT completed for table: {table_name}")
        
    except Exception as e:
        logger.error(f"Error processing {table_name}: {str(e)}")
        raise

def train_mlflow_model():
    """Train collaborative filtering model in MLflow"""
    logger.info("=" * 60)
    logger.info("Starting model training in MLflow...")
    logger.info("=" * 60)

    # Training server URL (port 5001 internal of mlflow-server container)
    training_url = "http://mlflow-server:5001/train"

    try:
        # Call training API on MLflow server
        logger.info(f"Calling training endpoint: {training_url}")

        response = requests.post(
            training_url,
            timeout=300  # 5 minutes for training
        )

        result = response.json()

        if response.status_code == 200:
            logger.info("=" * 60)
            logger.info("✓ Model trained successfully!")
            logger.info(f"  Status: {result.get('status')}")
            logger.info(f"  Message: {result.get('message')}")
            if result.get('output'):
                # Show last lines of output
                output_lines = result.get('output', '').strip().split('\n')
                for line in output_lines[-10:]:
                    if line.strip():
                        logger.info(f"  {line}")
            logger.info("=" * 60)
        else:
            error_msg = result.get('message', 'Unknown error')
            error_detail = result.get('error', '')
            logger.error(f"Error in training: {error_msg}")
            if error_detail:
                logger.error(f"Detail: {error_detail}")
            raise Exception(f"Training failed: {error_msg}")

    except requests.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to training server: {str(e)}")
        logger.error("Check if mlflow-server container is running")
        raise
    except requests.exceptions.Timeout:
        logger.error("Timeout waiting for training response")
        raise
    except Exception as e:
        logger.error(f"Error training model: {str(e)}")
        raise

# Tasks
create_db_task = PythonOperator(
    task_id='create_clickhouse_databases',
    python_callable=create_clickhouse_databases,
    dag=dag,
)

load_users_task = PythonOperator(
    task_id='load_users',
    python_callable=extract_and_load_users,
    dag=dag,
)

with TaskGroup("extract_load", dag=dag) as extract_load_group:
    for table in ['movies', 'watched_movies', 'ratings']:
        PythonOperator(
            task_id=f"load_{table}",
            python_callable=extract_and_load_table,
            op_kwargs={'table_name': table},
            dag=dag,
        )

# Task: Run dbt
dbt_task = BashOperator(
    task_id='run_dbt_transformations',
    bash_command='curl -X POST http://webhook-server:5000/webhook/manual || echo "Webhook not available"',
    dag=dag,
)

# Task: Train MLflow model
train_model_task = PythonOperator(
    task_id='train_mlflow_model',
    python_callable=train_mlflow_model,
    dag=dag,
)

# Dependencies
create_db_task >> load_users_task >> extract_load_group >> dbt_task >> train_model_task

if __name__ == "__main__":
    dag.cli()
