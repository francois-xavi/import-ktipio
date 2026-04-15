# Docker Setup Guide - BTP Company Enrichment Pipeline

## Overview

This guide explains how to containerize and run the KTIPIO company enrichment pipeline using Docker.

---

## Prerequisites

- **Docker** (version 20.10+)
- **Docker Compose** (version 2.0+)
- **Neon PostgreSQL Database** (connection URL)
- **4GB RAM minimum** (2GB recommended for container)

## Installation

### On Windows (WSL2)

```bash
# Install Docker Desktop from https://www.docker.com/products/docker-desktop
# Docker Desktop automatically installs Docker and Docker Compose
docker --version
docker-compose --version
```

### On Linux

```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

---

## Quick Start

### 1. Set Up Environment

Create a `.env` file in the project directory:

```bash
# Linux/Mac
cp .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env
```

Edit `.env` and add your Neon database URL:

```env
NEON_DATABASE_URL=postgresql://user:password@host.neon.tech:5432/dbname?sslmode=require
```

### 2. Build the Docker Image

```bash
# Build the image
docker-compose build

# Or build with specific tag
docker build -t ktipio/enrichment:v1.0 .
```

### 3. Run the Enrichment

#### Test with 1 company (dry-run):

```bash
docker-compose run enrichment-worker --limit 1 --dry-run
```

#### Process 50 companies:

```bash
docker-compose run enrichment-worker --limit 50 --batch-size 25 --delay 10
```

#### Run batch processing:

```bash
docker-compose run enrichment-worker --batch-size 100 --max-batches 5 --delay 5
```

#### Run with all defaults (production):

```bash
docker-compose run enrichment-worker
```

---

## Available Commands

### Run diagnostics

```bash
# Check database columns and data quality
docker-compose run enrichment-worker \
  python check_db_columns.py
```

### View enrichment script help

```bash
docker-compose run enrichment-worker --help
```

### Common enrichment scenarios

```bash
# Test mode (1 company, no database writes)
docker-compose run enrichment-worker --limit 1 --dry-run

# Safe mode (small batches, slow processing)
docker-compose run enrichment-worker --batch-size 10 --delay 20 --api-concurrent 3

# Balanced mode (moderate speed/safety)
docker-compose run enrichment-worker --batch-size 50 --delay 10 --api-concurrent 5

# Production mode (fast, with good stability)
docker-compose run enrichment-worker --batch-size 100 --delay 5 --api-concurrent 5

# Resume from specific offset
docker-compose run enrichment-worker --offset 1000 --batch-size 50

# Limit total companies processed
docker-compose run enrichment-worker --limit 5000 --batch-size 100
```

---

## Docker Compose Services

### enrichment-worker

Main service for running the enrichment pipeline.

**Features:**
- Runs in headless mode (no browser UI)
- Automatic Playwright browser setup
- PostgreSQL connection pooling
- Resource limits (2 CPU, 2GB RAM)
- Health checks

**Environment Variables:**
- `NEON_DATABASE_URL` - PostgreSQL connection string
- `PYTHONUNBUFFERED` - Real-time logging
- `PLAYWRIGHT_HEADLESS` - Headless mode (required)

**Volumes:**
- `/app/data` - Data caching
- `/app/logs` - Application logs

---

## Docker Image Details

### Image Specifications

- **Base:** Python 3.12-slim
- **Size:** ~1.2GB (after optimization)
- **Build Time:** ~5-10 minutes (first build)

### Installed Components

**System packages:**
- Playwright dependencies (chromium runtime)
- PostgreSQL client
- CA certificates for HTTPS

**Python packages:**
- playwright==1.58.0
- psycopg2-binary==2.9.11
- httpx==0.28.1
- nest-asyncio==1.6.0
- All dependencies from requirements.txt

### Multi-stage Build

The Dockerfile uses multi-stage build to:
1. Compile dependencies in a builder stage
2. Copy only necessary files to runtime stage
3. Reduce final image size by ~40%

---

## Production Deployment

### Docker Hub

Push to Docker Hub for easy sharing:

```bash
# Tag the image
docker tag ktipio/enrichment:latest your-username/ktipio-enrichment:v1.0

# Push to Docker Hub
docker push your-username/ktipio-enrichment:v1.0
```

### Environment Configuration

For production, use environment files:

```bash
# Create production environment file
cat > .env.production << EOF
NEON_DATABASE_URL=postgresql://...your-production-db...
LOG_LEVEL=INFO
EOF

# Use it with docker-compose
docker-compose --env-file .env.production up -d
```

### Kubernetes (Optional)

For Kubernetes deployment, create a simple pod:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: enrichment-worker
spec:
  containers:
  - name: enrichment
    image: ktipio/enrichment:v1.0
    env:
    - name: NEON_DATABASE_URL
      valueFrom:
        secretKeyRef:
          name: db-credentials
          key: url
    resources:
      requests:
        memory: "1Gi"
        cpu: "1"
      limits:
        memory: "2Gi"
        cpu: "2"
```

---

## Monitoring and Logs

### View Real-time Logs

```bash
# Stream logs while running
docker-compose logs -f enrichment-worker

# View last 100 lines
docker-compose logs --tail 100 enrichment-worker
```

### Access Container Shell

```bash
# Debug inside the container
docker-compose run enrichment-worker /bin/bash
```

### Check Container Status

```bash
# List running containers
docker-compose ps

# Inspect container details
docker inspect ktipio-enrichment-worker
```

---

## Troubleshooting

### Image Won't Build

**Error:** `E: Unable to locate package libnss3`

**Solution:** Update APT cache and try again:
```bash
docker system prune -a
docker-compose build --no-cache
```

### Database Connection Failed

**Error:** `could not translate host name 'host' to address`

**Solution:** 
1. Verify `NEON_DATABASE_URL` is set: `echo $NEON_DATABASE_URL`
2. Test connection: `docker-compose run enrichment-worker python check_db_columns.py`
3. Check Neon firewall settings

### Out of Memory

**Error:** `Killed` (container exits with code 137)

**Solution:**
1. Increase Docker memory allocation
2. Reduce batch size: `--batch-size 25` instead of 100
3. Increase delay between requests: `--delay 15`

### Playwright Browser Issues

**Error:** `chromium not found`

**Solution:** Rebuild with browser cache:
```bash
docker-compose build --no-cache --build-arg PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0
```

---

## Performance Optimization

### Resource Limits

Current settings in docker-compose.yml:

```yaml
limits:
  cpus: '2'
  memory: 2G
reservations:
  cpus: '1'
  memory: 1G
```

Adjust based on your system:

```bash
# More powerful server
docker-compose up -d --compatibility

# Edit docker-compose.yml to increase limits
```

### Network Optimization

For better API performance:

```bash
# Use host network (Linux only)
docker-compose run --network host enrichment-worker ...

# Or set DNS servers
docker-compose run --dns 8.8.8.8 enrichment-worker ...
```

---

## Advanced Usage

### Run Multiple Workers in Parallel

```bash
# Create multiple instances
docker-compose run -d --name worker-1 enrichment-worker --offset 0 --batch-size 100
docker-compose run -d --name worker-2 enrichment-worker --offset 100 --batch-size 100
docker-compose run -d --name worker-3 enrichment-worker --offset 200 --batch-size 100

# Monitor all
docker-compose logs -f
```

### Scheduled Runs (Cron)

```bash
# Create a cron job that runs enrichment daily
cat > /etc/cron.d/ktipio-enrichment << EOF
# Run enrichment every day at 2 AM
0 2 * * * /usr/bin/docker-compose -f /path/to/siren/docker-compose.yml run enrichment-worker --batch-size 100 --delay 5
EOF
```

### CI/CD Integration

GitHub Actions example:

```yaml
name: Enrichment Pipeline

on:
  schedule:
    - cron: '0 2 * * *'

jobs:
  enrich:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: docker/setup-buildx-action@v2
      - uses: docker/build-push-action@v4
        with:
          context: .
          push: false
          load: true
          tags: ktipio/enrichment:latest
      - name: Run enrichment
        env:
          NEON_DATABASE_URL: ${{ secrets.NEON_DATABASE_URL }}
        run: |
          docker-compose run enrichment-worker --batch-size 100 --delay 5
```

---

## Maintenance

### Update Dependencies

```bash
# Update Python dependencies
pip install -U -r requirements.txt

# Rebuild Docker image
docker-compose build --no-cache
```

### Clean Up

```bash
# Remove unused images
docker image prune -a

# Remove all stopped containers
docker container prune

# Remove dangling volumes
docker volume prune

# Full cleanup (WARNING: removes all Docker data)
docker system prune -a --volumes
```

### Backup Logs

```bash
# Export logs to file
docker-compose logs enrichment-worker > enrichment-$(date +%Y%m%d).log

# Or use docker logs
docker logs ktipio-enrichment-worker > enrichment-debug.log
```

---

## Configuration Examples

### Development Setup

```yaml
# Override in docker-compose.override.yml
version: '3.9'
services:
  enrichment-worker:
    environment:
      LOG_LEVEL: DEBUG
    deploy:
      resources:
        limits:
          memory: 4G
    command: --limit 10 --dry-run
```

### Production Setup

```bash
# Use secrets for database URL
docker secret create db-url .secrets/neon-url

# Run with secret
docker run \
  --secret db-url \
  -e NEON_DATABASE_URL_FILE=/run/secrets/db-url \
  ktipio/enrichment:v1.0
```

---

## Support and Documentation

- **Main Script Help:** `docker-compose run enrichment-worker --help`
- **Database Diagnostic:** `docker-compose run enrichment-worker python check_db_columns.py`
- **Docker Docs:** https://docs.docker.com/
- **Docker Compose Docs:** https://docs.docker.com/compose/

---

## Summary

| Task | Command |
|------|---------|
| Build image | `docker-compose build` |
| Test (1 company) | `docker-compose run enrichment-worker --limit 1 --dry-run` |
| Process batch | `docker-compose run enrichment-worker --batch-size 50` |
| Check database | `docker-compose run enrichment-worker python check_db_columns.py` |
| View logs | `docker-compose logs -f` |
| Debug shell | `docker-compose run enrichment-worker /bin/bash` |
| Push to registry | `docker push your-username/ktipio-enrichment:latest` |

**All set! Your enrichment pipeline is now containerized and ready for deployment.** 🐳

