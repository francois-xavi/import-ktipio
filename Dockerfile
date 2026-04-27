FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright + toutes ses dépendances système automatiquement
RUN playwright install --with-deps chromium

COPY google_reviews_worker.py .
COPY batch_enrich.py .
COPY check_db_columns.py .
COPY enrich_qualibat_rge_db.py .
COPY enrich_qualibat_scraper.py .

# Entrypoint flexible : SCRIPT env var sélectionne le script à lancer
# Par défaut : google_reviews_worker.py
RUN printf '#!/bin/bash\nif [ -z "$NEON_DATABASE_URL" ]; then echo "ERROR: NEON_DATABASE_URL not set"; exit 1; fi\nSCRIPT="${SCRIPT:-google_reviews_worker.py}"\nexec python "$SCRIPT" "$@"\n' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["/app/entrypoint.sh"]
