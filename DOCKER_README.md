# 🐳 Docker Setup - BTP Company Enrichment Pipeline

Complete containerization of the KTIPIO enrichment pipeline with multi-stage builds, health checks, and production-ready configuration.

## What's Included

### Docker Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Multi-stage build for optimized image (~1.2GB) |
| `docker-compose.yml` | Service orchestration with environment management |
| `.dockerignore` | Exclude unnecessary files from build context |
| `.env.example` | Environment variables template |

### Helper Scripts

| File | Purpose |
|------|---------|
| `docker-run.sh` | Linux/Mac helper script with commands |
| `docker-run.bat` | Windows helper script with commands |

### Documentation

| File | Purpose |
|------|---------|
| `DOCKER_QUICKSTART.md` | 5-minute setup guide (start here!) |
| `DOCKER_SETUP.md` | Complete reference documentation |
| `DOCKER_README.md` | This file |

## Quick Start (30 seconds)

```bash
# 1. Set up environment
cp .env.example .env
# Edit .env and add your NEON_DATABASE_URL

# 2. Build image
docker-compose build

# 3. Test it
docker-compose run --rm enrichment-worker --limit 1 --dry-run

# 4. Run enrichment
docker-compose run --rm enrichment-worker --batch-size 50 --delay 10
```

**New to Docker?** Start with `DOCKER_QUICKSTART.md` instead!

## Architecture

```
┌─────────────────────────────────────────┐
│   Docker Image (1.2GB)                  │
├─────────────────────────────────────────┤
│ Runtime: Python 3.12-slim               │
├─────────────────────────────────────────┤
│ Key Dependencies:                       │
│  • Playwright 1.58.0 + Chromium         │
│  • PostgreSQL client (psycopg2)         │
│  • HTTP client (httpx)                  │
│  • Async framework (nest-asyncio)       │
├─────────────────────────────────────────┤
│ Scripts:                                │
│  • google_reviews_worker.py             │
│  • batch_enrich.py                      │
│  • check_db_columns.py                  │
├─────────────────────────────────────────┤
│ Configuration:                          │
│  • Headless mode (required)             │
│  • Health checks enabled                │
│  • Resource limits (2CPU, 2GB RAM)      │
└─────────────────────────────────────────┘
         ↓
    External Services
         ↓
┌─────────────────────────────────────────┐
│   Neon PostgreSQL Database              │
│   (houses the company data)             │
└─────────────────────────────────────────┘
```

## File Breakdown

### Dockerfile

**Features:**
- Multi-stage build (reduces image size by ~40%)
- Minimal base image (python:3.12-slim)
- All Playwright dependencies included
- Chromium browser pre-installed
- Health checks configured
- Secure entrypoint script

**Build Time:** 5-10 minutes (first build with browser)
**Image Size:** ~1.2GB (optimized)
**Layers:** 15+ (cached for faster rebuilds)

### docker-compose.yml

**Services:**
- `enrichment-worker` - Main enrichment service

**Configuration:**
- Environment variables management
- Volume mounts for data/logs
- Resource limits (2CPU, 2GB RAM)
- Health checks
- Network isolation
- Restart policy

**Optional Services:**
- PostgreSQL (commented out, for local testing)

### Helper Scripts

**docker-run.sh (Linux/Mac):**
- Bash script with colored output
- Automatic checks (Docker, environment)
- Helper commands for common tasks
- Error handling and validation

**docker-run.bat (Windows):**
- Batch script for Command Prompt
- Windows-compatible commands
- Same functionality as shell script

## Available Commands

### Using docker-compose directly

```bash
# Test
docker-compose run --rm enrichment-worker --limit 1 --dry-run

# Enrich companies
docker-compose run --rm enrichment-worker --batch-size 50 --delay 10

# Batch processing
docker-compose run --rm enrichment-worker --batch-size 100 --max-batches 5

# Check database
docker-compose run --rm enrichment-worker python check_db_columns.py

# View help
docker-compose run --rm enrichment-worker --help

# Shell access
docker-compose run --rm enrichment-worker /bin/bash
```

### Using helper scripts (Recommended)

**Linux/Mac:**
```bash
./docker-run.sh build          # Build image
./docker-run.sh test           # Test setup
./docker-run.sh database       # Check DB
./docker-run.sh enrich 50 10   # Run enrichment
./docker-run.sh batch 50 5 10  # Run batch
./docker-run.sh logs           # View logs
./docker-run.sh clean          # Cleanup
```

**Windows:**
```batch
docker-run.bat build
docker-run.bat test
docker-run.bat database
docker-run.bat enrich 50 10
docker-run.bat batch 50 5 10
docker-run.bat logs
docker-run.bat clean
```

## Configuration

### Environment Variables

**Required:**
- `NEON_DATABASE_URL` - PostgreSQL connection string

**Optional:**
- `LOG_LEVEL` - Logging level (DEBUG, INFO, WARNING, ERROR)
- `PYTHONUNBUFFERED` - Real-time log output
- `PLAYWRIGHT_HEADLESS` - Headless mode (always 1)

### Resource Limits

Default in `docker-compose.yml`:
```yaml
limits:
  cpus: '2'        # Max 2 CPU cores
  memory: 2G       # Max 2GB RAM
```

Adjust for your system:
- **More powerful:** Increase CPU/memory for faster processing
- **Limited resources:** Decrease batch size instead of limits

### Database Connection

Supports only PostgreSQL via Neon:

```env
# Format
postgresql://[user]:[password]@[host]:[port]/[database]?sslmode=require

# Example
postgresql://user@host.neon.tech:5432/companies?sslmode=require
```

## Production Deployment

### Push to Docker Hub

```bash
# Tag image
docker tag ktipio/enrichment:latest username/ktipio-enrichment:v1.0

# Push
docker push username/ktipio-enrichment:v1.0
```

### Run on Server

```bash
# Pull and run
docker run \
  --env NEON_DATABASE_URL=postgresql://... \
  --memory 2g \
  --cpus 2 \
  username/ktipio-enrichment:v1.0 \
  --batch-size 100 --delay 5
```

### Kubernetes (if needed)

See `DOCKER_SETUP.md` for Kubernetes pod manifest example.

## Monitoring

### View Logs

```bash
# Real-time logs
docker-compose logs -f enrichment-worker

# Last 100 lines
docker-compose logs --tail 100

# Specific time range
docker-compose logs --since 10m
```

### Health Check

```bash
# View container status
docker-compose ps

# Check health
docker inspect ktipio-enrichment-worker | grep -A 5 "Health"
```

## Troubleshooting

### Build Issues

```bash
# Rebuild without cache
docker-compose build --no-cache

# Check build logs
docker-compose build --progress=plain

# Prune Docker system
docker system prune -a
```

### Connection Issues

```bash
# Test database connection
docker-compose run --rm enrichment-worker python check_db_columns.py

# Debug environment
docker-compose run --rm enrichment-worker env | grep NEON
```

### Resource Issues

```bash
# Check Docker resources
docker stats

# Reduce batch size
docker-compose run --rm enrichment-worker \
  --batch-size 25 --delay 20

# Increase Docker memory allocation
# Docker Desktop → Settings → Resources → Memory
```

## Performance Tips

1. **Batch Size:** Start with 50, increase to 100 for more speed
2. **Delay:** Use 5-10 seconds to avoid API throttling
3. **API Concurrent:** Default 5 is safe, max 10 if needed
4. **Memory:** Allocate 2GB minimum in Docker Desktop

## File Size Reference

| Component | Size |
|-----------|------|
| Dockerfile | 2.4 KB |
| docker-compose.yml | 1.9 KB |
| .dockerignore | 1.1 KB |
| .env.example | 250 B |
| Built image | 1.2 GB |

## Next Steps

1. **Get started:** Read `DOCKER_QUICKSTART.md`
2. **Full reference:** Read `DOCKER_SETUP.md`
3. **Troubleshoot:** Check "Troubleshooting" section above
4. **Deploy:** See "Production Deployment" in `DOCKER_SETUP.md`

## Support

- **Docker docs:** https://docs.docker.com/
- **Docker Compose:** https://docs.docker.com/compose/
- **Playwright:** https://playwright.dev/
- **PostgreSQL:** https://www.postgresql.org/

## Summary

| Aspect | Details |
|--------|---------|
| **Time to setup** | 5 minutes |
| **Time to build** | 5-10 minutes |
| **Image size** | 1.2 GB |
| **Memory required** | 2 GB |
| **CPU cores** | 2 recommended |
| **Database** | Neon PostgreSQL |
| **Browser** | Headless Chromium |
| **Python version** | 3.12 |
| **Production ready** | Yes |

---

**Ready to dockerize?** Start with `DOCKER_QUICKSTART.md`! 🚀

