# Multi-stage Dockerfile for BTP Company Enrichment Pipeline

# Stage 1: Builder
FROM python:3.12-slim as builder

WORKDIR /app

# Install system dependencies for Playwright and compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Create a virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Install Playwright browsers (needed for headless mode)
RUN playwright install chromium

# Stage 2: Runtime
FROM python:3.12-slim

WORKDIR /app

# Install runtime dependencies for Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libxss1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgcc-s1 \
    libglib2.0-0 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxrender1 \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_HEADLESS=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Copy application code
COPY google_reviews_worker.py .
COPY batch_enrich.py .
COPY check_db_columns.py .
COPY .env.example .env.example 2>/dev/null || true

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Check if database URL is set\n\
if [ -z "$NEON_DATABASE_URL" ] && [ -z "$DATABASE_URL" ]; then\n\
    echo "ERROR: NEON_DATABASE_URL or DATABASE_URL environment variable not set"\n\
    exit 1\n\
fi\n\
\n\
# Run the enrichment script with provided arguments\n\
exec python google_reviews_worker.py "$@"\n\
' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import google_reviews_worker; print(\"OK\")" || exit 1

# Default command
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--help"]
