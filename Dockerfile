FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright + toutes ses dépendances système automatiquement
RUN playwright install --with-deps chromium

COPY google_reviews_worker.py .
COPY batch_enrich.py .
COPY check_db_columns.py .

RUN printf '#!/bin/bash\nif [ -z "$NEON_DATABASE_URL" ]; then echo "ERROR: NEON_DATABASE_URL not set"; exit 1; fi\nexec python google_reviews_worker.py "$@"\n' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/app/entrypoint.sh"]
