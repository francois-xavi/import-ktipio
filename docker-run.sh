#!/bin/bash
# Helper script for running Docker containers for the enrichment pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Functions
print_header() {
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    print_success "Docker and Docker Compose are installed"
}

check_env() {
    if [ ! -f "$SCRIPT_DIR/.env" ]; then
        print_error ".env file not found"
        print_info "Creating .env from .env.example..."
        cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
        print_info "Please edit .env and add your NEON_DATABASE_URL"
        exit 1
    fi

    if ! grep -q "NEON_DATABASE_URL=" "$SCRIPT_DIR/.env"; then
        print_error "NEON_DATABASE_URL not set in .env"
        exit 1
    fi

    print_success ".env file is configured"
}

build_image() {
    print_header "Building Docker Image"
    cd "$SCRIPT_DIR"
    docker-compose build --progress=plain
    print_success "Docker image built successfully"
}

test_setup() {
    print_header "Testing Setup (1 company, dry-run)"
    cd "$SCRIPT_DIR"
    docker-compose run --rm enrichment-worker --limit 1 --dry-run
    print_success "Setup test completed"
}

check_database() {
    print_header "Checking Database Configuration"
    cd "$SCRIPT_DIR"
    docker-compose run --rm enrichment-worker python check_db_columns.py
    print_success "Database check completed"
}

run_enrichment() {
    local batch_size=${1:-50}
    local delay=${2:-10}

    print_header "Running Enrichment Pipeline"
    print_info "Batch size: $batch_size"
    print_info "Delay: $delay seconds"

    cd "$SCRIPT_DIR"
    docker-compose run --rm enrichment-worker \
        --batch-size "$batch_size" \
        --delay "$delay" \
        --api-concurrent 5

    print_success "Enrichment completed"
}

run_batch() {
    local batch_size=${1:-50}
    local max_batches=${2:-5}
    local delay=${3:-10}

    print_header "Running Batch Enrichment"
    print_info "Batch size: $batch_size"
    print_info "Max batches: $max_batches"
    print_info "Delay: $delay seconds"

    cd "$SCRIPT_DIR"
    docker-compose run --rm enrichment-worker \
        --batch-size "$batch_size" \
        --max-batches "$max_batches" \
        --delay "$delay"

    print_success "Batch enrichment completed"
}

view_logs() {
    print_header "Container Logs"
    cd "$SCRIPT_DIR"
    docker-compose logs -f enrichment-worker
}

cleanup() {
    print_header "Cleanup"
    cd "$SCRIPT_DIR"
    docker-compose down
    print_success "Containers stopped"
}

show_help() {
    cat << EOF
${BLUE}BTP Company Enrichment Pipeline - Docker Helper${NC}

Usage: ./docker-run.sh [COMMAND] [OPTIONS]

Commands:
    check           Check Docker installation
    setup           Check environment configuration
    build           Build Docker image
    test            Test setup (1 company, dry-run)
    database        Check database configuration
    enrich          Run enrichment (default: 50 batch size, 10s delay)
    batch           Run batch enrichment (default: 5 batches, 50 batch size)
    logs            View container logs
    clean           Stop and remove containers
    help            Show this help message

Examples:

    # Build the Docker image
    ./docker-run.sh build

    # Test the setup
    ./docker-run.sh test

    # Check database configuration
    ./docker-run.sh database

    # Run enrichment with custom settings
    ./docker-run.sh enrich 100 15
    # (batch-size=100, delay=15s)

    # Run batch enrichment
    ./docker-run.sh batch 50 10 5
    # (batch-size=50, max-batches=10, delay=5s)

    # View logs
    ./docker-run.sh logs

    # Cleanup
    ./docker-run.sh clean

Full documentation: DOCKER_SETUP.md

EOF
}

# Main script
main() {
    case "${1:-help}" in
        check)
            check_docker
            ;;
        setup)
            check_docker
            check_env
            ;;
        build)
            check_docker
            check_env
            build_image
            ;;
        test)
            check_docker
            check_env
            test_setup
            ;;
        database)
            check_docker
            check_env
            check_database
            ;;
        enrich)
            check_docker
            check_env
            run_enrichment "${2:-50}" "${3:-10}"
            ;;
        batch)
            check_docker
            check_env
            run_batch "${2:-50}" "${3:-5}" "${4:-10}"
            ;;
        logs)
            check_docker
            view_logs
            ;;
        clean)
            check_docker
            cleanup
            ;;
        help)
            show_help
            ;;
        *)
            print_error "Unknown command: $1"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
