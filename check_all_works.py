#!/usr/bin/env python3
"""
Dataflix - Functionality Check Script
Validates all services and system components
"""

import requests
import psycopg2
from clickhouse_driver import Client
import time
import sys
import json
from datetime import datetime

# Colors
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}  {text}{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")

def print_success(text):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")

def print_error(text):
    print(f"{Colors.RED}✗ {text}{Colors.END}")

def print_info(text):
    print(f"{Colors.YELLOW}ℹ {text}{Colors.END}")

def check_service(name, url, method='GET', timeout=5):
    """Check if a service is responding"""
    try:
        if method == 'GET':
            response = requests.get(url, timeout=timeout)
        else:
            response = requests.post(url, timeout=timeout)
        
        if response.status_code < 400:
            print_success(f"{name} is responding ({response.status_code})")
            return True
        else:
            print_error(f"{name} returned error ({response.status_code})")
            return False
    except Exception as e:
        print_error(f"{name} is not responding: {str(e)}")
        return False

def check_postgres():
    """Check PostgreSQL"""
    print_header("Checking PostgreSQL")
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            user='dataflix',
            password='dataflix123',
            database='dataflix_db',
            connect_timeout=5
        )
        
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        
        print_success(f"PostgreSQL connected successfully")
        print_info(f"Tables found: {', '.join(table_names)}")
        
        # Check data
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        print_info(f"Users: {user_count}")
        
        cursor.execute("SELECT COUNT(*) FROM movies")
        movie_count = cursor.fetchone()[0]
        print_info(f"Movies: {movie_count}")
        
        cursor.execute("SELECT COUNT(*) FROM watched_movies")
        watched_count = cursor.fetchone()[0]
        print_info(f"Watched movies: {watched_count}")
        
        cursor.execute("SELECT COUNT(*) FROM ratings")
        ratings_count = cursor.fetchone()[0]
        print_info(f"Ratings: {ratings_count}")
        
        cursor.close()
        conn.close()
        
        return True
    
    except Exception as e:
        print_error(f"Error connecting to PostgreSQL: {str(e)}")
        return False

def check_clickhouse():
    """Check ClickHouse"""
    print_header("Checking ClickHouse")
    
    try:
        client = Client('localhost', port=9000, user='default', password='clickhouse123')
        
        # Check databases
        result = client.execute("SHOW DATABASES")
        databases = [db[0] for db in result]
        
        print_success(f"ClickHouse connected successfully")
        print_info(f"Databases: {', '.join(databases)}")
        
        # Check raw tables
        if 'raw' in databases:
            result = client.execute("SHOW TABLES FROM raw")
            tables = [t[0] for t in result]
            print_info(f"Tables in raw: {', '.join(tables)}")
        
        # Check analytics tables
        if 'analytics' in databases:
            result = client.execute("SHOW TABLES FROM analytics")
            tables = [t[0] for t in result]
            print_info(f"Tables in analytics: {', '.join(tables)}")
        
        return True
    
    except Exception as e:
        print_error(f"Error connecting to ClickHouse: {str(e)}")
        return False

def check_airflow():
    """Check Airflow"""
    print_header("Checking Airflow")
    
    return check_service("Airflow Webserver", "http://localhost:8080/health")

def check_dbt_webhook():
    """Check dbt Webhook Server"""
    print_header("Checking dbt Webhook Server")
    
    return check_service("dbt Webhook Server", "http://localhost:5001/health")

def check_minio():
    """Check Minio"""
    print_header("Checking Minio")
    
    return check_service("Minio Console", "http://localhost:9003/")

def check_mlflow():
    """Check MLflow"""
    print_header("Checking MLflow")
    
    return check_service("MLflow Server", "http://localhost:5000/health")

def check_backend():
    """Check Backend API"""
    print_header("Checking Backend API")
    
    return check_service("Backend API", "http://localhost:5002/health")

def check_superset():
    """Check Superset"""
    print_header("Checking Apache Superset")
    
    return check_service("Superset", "http://localhost:8088/health")

def test_etl_pipeline():
    """Test ETL pipeline"""
    print_header("Testing ETL Pipeline")
    
    try:
        # Check if Airflow is running
        response = requests.get("http://localhost:8080/api/v1/dags", timeout=5)
        
        if response.status_code == 200:
            dags = response.json().get('dags', [])
            dag_ids = [dag['dag_id'] for dag in dags]
            
            if 'elt_pipeline' in dag_ids:
                print_success("DAG 'elt_pipeline' found in Airflow")
                return True
            else:
                print_error("DAG 'elt_pipeline' not found")
                print_info(f"Available DAGs: {', '.join(dag_ids)}")
                return False
        else:
            print_error(f"Error connecting to Airflow API: {response.status_code}")
            return False
    
    except Exception as e:
        print_error(f"Error testing ETL: {str(e)}")
        return False

def test_backend_endpoints():
    """Test Backend Endpoints"""
    print_header("Testing Backend Endpoints")
    
    endpoints = [
        ('GET', '/users', 'List users'),
        ('GET', '/movies', 'List movies'),
    ]
    
    success_count = 0
    
    for method, endpoint, description in endpoints:
        try:
            url = f"http://localhost:5002{endpoint}"
            
            if method == 'GET':
                response = requests.get(url, timeout=5)
            else:
                response = requests.post(url, timeout=5)
            
            if response.status_code < 400:
                data = response.json()
                if 'data' in data:
                    count = len(data['data']) if isinstance(data['data'], list) else 1
                    print_success(f"{description}: {count} items")
                    success_count += 1
                else:
                    print_success(f"{description}")
                    success_count += 1
            else:
                print_error(f"{description}: {response.status_code}")
        
        except Exception as e:
            print_error(f"{description}: {str(e)}")
    
    return success_count == len(endpoints)

def main():
    """Main function"""
    print_header("Dataflix - Functionality Check")
    
    print_info(f"Starting checks at {datetime.now().isoformat()}")
    
    # Wait a bit for services to start
    print_info("Waiting for services to start...")
    time.sleep(5)
    
    results = {}
    
    # Check services
    results['PostgreSQL'] = check_postgres()
    results['ClickHouse'] = check_clickhouse()
    results['Airflow'] = check_airflow()
    results['dbt Webhook'] = check_dbt_webhook()
    results['Minio'] = check_minio()
    results['MLflow'] = check_mlflow()
    results['Backend API'] = check_backend()
    results['Superset'] = check_superset()
    
    # Functional tests
    results['ETL Pipeline'] = test_etl_pipeline()
    results['Backend Endpoints'] = test_backend_endpoints()
    
    # Summary
    print_header("Check Summary")
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed
    
    for service, status in results.items():
        if status:
            print_success(f"{service}")
        else:
            print_error(f"{service}")
    
    print_info(f"\nTotal: {passed}/{total} checks passed")
    
    if failed == 0:
        print_success("All services are working correctly!")
        print_info("\nYour Dataflix system is 100% functional!")
        print_info("Next steps:")
        print_info("  1. Access Airflow at http://localhost:8080")
        print_info("  2. Access MLflow at http://localhost:5003")
        print_info("  3. Access Superset at http://localhost:8088")
        print_info("  4. Configure dashboards: python3 scripts/setup_superset_dataflix.py")
        print_info("  5. Run CLI: python3 cli/main.py")
        return 0
    else:
        print_error(f"{failed} services failed check")
        return 1

if __name__ == '__main__':
    sys.exit(main())
