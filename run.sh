#!/bin/bash

# ============================================================================
# Script: run.sh
# Description: Starts the Dataflix project with Docker Compose
# ============================================================================

set -e

# Ensure local python bin is in PATH (for tools like uv)
if command -v python3 &> /dev/null; then
    USER_BASE=$(python3 -m site --user-base)
    if [ -d "$USER_BASE/bin" ]; then
        export PATH="$USER_BASE/bin:$PATH"
    fi
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Functions
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[⚠]${NC} $1"; }

print_header() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  🎬 Dataflix - Collaborative Filtering System${NC}             ${BLUE}║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Header
print_header

# Stop previous containers
log_info "Stopping previous containers..."
docker-compose down -v 2>/dev/null || true
sleep 2

# Start Docker Compose
log_info "Starting Docker Compose..."
docker-compose up -d 2>&1 | grep -E "Creating|Created|Starting|Started|Error" || true

# Wait for containers
log_info "Waiting for containers to be ready (120 seconds)..."
sleep 120

# Container status
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Container Status${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

CONTAINERS=("postgres-dataflix" "clickhouse-dataflix" "airflow-db" "airflow-webserver-dataflix" "airflow-scheduler" "webhook-server" "mlflow-server" "backend" "superset")

for container in "${CONTAINERS[@]}"; do
    if docker ps --filter "name=$container" --filter "status=running" -q | grep -q .; then
        log_success "Container '$container' is running"
    else
        log_warning "Container '$container' is not running"
    fi
done

# Credentials
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔑 Access Credentials${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${YELLOW}PostgreSQL Source (OLTP):${NC}"
echo "  Host: localhost | Port: 5432 | User: dataflix | Password: dataflix123 | DB: dataflix_db"
echo ""
echo -e "${YELLOW}ClickHouse (Data Warehouse):${NC}"
echo "  HTTP: http://localhost:8123 | Native: localhost:9000 | User: default | Password: clickhouse123"
echo ""
echo -e "${YELLOW}Apache Airflow (Orchestration):${NC}"
echo "  URL: http://localhost:8080 | User: admin | Password: admin"
echo ""
echo -e "${YELLOW}Apache Superset (Visualization):${NC}"
echo "  URL: http://localhost:8088 | User: admin | Password: admin"
echo ""
echo -e "${YELLOW}MLflow (Model Registry):${NC}"
echo "  URL: http://localhost:5003"
echo ""
echo -e "${YELLOW}Backend API:${NC}"
echo "  URL: http://localhost:5002"
echo ""
echo -e "${YELLOW}dbt Webhook Server:${NC}"
echo "  URL: http://localhost:5001 | Health: http://localhost:5001/health"
echo ""

# Run ELT pipeline and dbt transformations
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Running ELT Pipeline${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

log_info "Triggering Airflow DAG to load data..."
docker exec airflow-webserver-dataflix airflow dags trigger elt_pipeline 2>/dev/null || log_warning "Could not trigger DAG"

log_info "Waiting for DAG execution (60 seconds)..."
sleep 60

log_info "Verifying dbt data in ClickHouse..."
if docker exec clickhouse-server clickhouse-client --query "SELECT count() FROM analytics.mart_movies" 2>/dev/null | grep -qE '^[1-9][0-9]*$'; then
    log_success "Data verification successful: dbt transformations data found in analytics.mart_movies"
else
    log_warning "Data verification failed: No data found in analytics.mart_movies"
fi

log_success "ELT pipeline completed"

# Configure Superset
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Configuring Superset${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

log_info "Setting up Superset with ClickHouse connection..."
uv run python scripts/setup_superset_dataflix.py 2>&1 || log_warning "Superset setup completed with warnings"

# Finish
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${GREEN}All services have been started successfully!${NC}"
echo ""
echo -e "${GREEN}Access URLs:${NC}"
echo '  Airflow:   http://localhost:8080 (admin/admin)'
echo '  Superset:  http://localhost:8088 (admin/admin)'
echo "  MLflow:    http://localhost:5003"
echo "  Backend:   http://localhost:5002"
echo "  ClickHouse: http://localhost:8123"
echo '  dbt Docs: http://localhost:5001/docs/#!/overview'
echo ""
echo -e "${GREEN}To verify everything is working:${NC}"
echo "  uv run python check_all_works.py"
echo ""
echo -e "${GREEN}To use the CLI:${NC}"
echo "  uv run python cli/main.py"
echo ""
echo -e "${GREEN}To stop services:${NC}"
echo "  ./stop.sh"
echo ""
