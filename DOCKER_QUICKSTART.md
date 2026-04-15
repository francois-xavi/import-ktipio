# Docker Quick Start

Get the enrichment pipeline running in Docker in 5 minutes!

## Prerequisites

- Docker Desktop installed ([download here](https://www.docker.com/products/docker-desktop))
- Your Neon PostgreSQL connection string ready

## 5-Minute Setup

### Step 1: Create `.env` file

```bash
# Copy the example
cp .env.example .env

# Edit and add your database URL
# On Windows:
notepad .env
# On Mac/Linux:
nano .env
```

Add your Neon database URL:
```env
NEON_DATABASE_URL=postgresql://user:password@host.neon.tech:5432/companies?sslmode=require
```

### Step 2: Build the image

```bash
docker-compose build
```

This takes 5-10 minutes on first run (includes installing Playwright).

### Step 3: Test it works

```bash
docker-compose run --rm enrichment-worker --limit 1 --dry-run
```

Should output enrichment details for 1 company without writing to database.

### Step 4: Run enrichment

```bash
# Process 50 companies
docker-compose run --rm enrichment-worker --batch-size 50 --delay 10
```

That's it! ✅

---

## Common Commands

```bash
# Test setup
docker-compose run --rm enrichment-worker --limit 1 --dry-run

# Check database connection
docker-compose run --rm enrichment-worker python check_db_columns.py

# Run enrichment (50 at a time)
docker-compose run --rm enrichment-worker --batch-size 50 --delay 10

# View logs
docker-compose logs -f

# Debug/shell access
docker-compose run --rm enrichment-worker /bin/bash

# Clean up
docker-compose down
```

---

## Windows Users

Use the helper batch script:

```batch
REM Build
docker-run.bat build

REM Test
docker-run.bat test

REM Run
docker-run.bat enrich 50 10

REM Batch
docker-run.bat batch 50 5 10
```

---

## Linux/Mac Users

Use the helper shell script:

```bash
# Make executable
chmod +x docker-run.sh

# Build
./docker-run.sh build

# Test
./docker-run.sh test

# Run
./docker-run.sh enrich 50 10

# Batch
./docker-run.sh batch 50 5 10
```

---

## Troubleshooting

### "Cannot connect to database"

1. Check `.env` file has `NEON_DATABASE_URL`
2. Verify URL format is correct
3. Test connection: `docker-compose run --rm enrichment-worker python check_db_columns.py`

### "Out of memory"

1. Increase Docker memory allocation (Docker Desktop → Settings → Resources)
2. Or reduce batch size: `--batch-size 25` instead of 100

### "Chromium not found"

```bash
# Rebuild with browsers
docker-compose build --no-cache
```

---

## Next Steps

- **Full documentation**: See `DOCKER_SETUP.md`
- **Production deployment**: See "Production Deployment" in `DOCKER_SETUP.md`
- **CI/CD integration**: See "CI/CD Integration" in `DOCKER_SETUP.md`

---

## Resource Usage

- **Image size**: ~1.2GB
- **Memory**: 2GB (configurable)
- **CPU**: 2 cores (configurable)
- **Network**: Depends on batch size and processing speed

---

## Performance

| Configuration | Speed | Stability | Cost |
|---|---|---|---|
| `--batch-size 25 --delay 20` | Slow | Very stable | Low |
| `--batch-size 50 --delay 10` | Medium | Stable | Medium |
| `--batch-size 100 --delay 5` | Fast | Good | Medium |

Start with `--batch-size 50 --delay 10` and adjust based on results.

---

## Success!

You now have a fully containerized enrichment pipeline! 🐳

Next: Push to Docker Hub or deploy to production.

