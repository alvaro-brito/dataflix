#!/bin/bash

# ============================================================================
# Dataflix - Script to stop all services
# ============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_header "Dataflix - Stopping Services"

# Stop containers
print_info "Stopping containers..."
docker-compose down

print_success "All services have been stopped"

# Option to remove volumes
read -p "Do you want to remove data volumes? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Removing volumes..."
    docker-compose down -v
    print_success "Volumes removed"
fi

echo ""
