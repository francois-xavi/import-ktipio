#!/usr/bin/env python3
"""
Script de RATTRAPAGE : synchronise companies depuis google_reviews.

Met à jour les colonnes suivantes dans companies pour toutes les entreprises
qui ont une ligne dans google_reviews mais dont reviews_enriched_at est NULL :

  - google_rating
  - google_review_count
  - telephone (si vide dans companies)
  - email (si vide dans companies)
  - site_web (si vide dans companies)
  - reviews_enriched_status = 'done'
  - reviews_enriched_at = NOW()

Utile après le bugfix qui n'écrivait pas dans companies.

Usage:
    docker compose run --rm --entrypoint python enrichment-worker \
        sync_companies_from_google_reviews.py
"""

import os
import sys
import psycopg2

DB_URL = os.getenv("NEON_DATABASE_URL")
if not DB_URL:
    print("❌ ERROR: NEON_DATABASE_URL non définie")
    sys.exit(1)


def main():
    print("=" * 70)
    print("  🔄 SYNC companies ← google_reviews")
    print("=" * 70)

    conn = psycopg2.connect(DB_URL, connect_timeout=30)
    cur = conn.cursor()

    # 1. Compter le travail
    print("\n[1/3] Comptage des lignes à synchroniser…")
    cur.execute("""
        SELECT COUNT(*)
        FROM google_reviews gr
        JOIN companies c ON c.siret = gr.siret
        WHERE c.reviews_enriched_at IS NULL
           OR c.reviews_enriched_at < gr.scraped_at;
    """)
    total = cur.fetchone()[0]
    print(f"   → {total:,} entreprises à synchroniser")

    if total == 0:
        print("\n✅ Rien à faire, tout est déjà synchronisé.")
        return

    # 2. Mise à jour en bulk
    print(f"\n[2/3] Mise à jour de companies (peut prendre quelques minutes)…")
    cur.execute("""
        UPDATE companies c SET
            google_rating           = COALESCE(gr.rating, c.google_rating),
            google_review_count     = COALESCE(gr.review_count, c.google_review_count),
            telephone               = COALESCE(NULLIF(gr.phone, ''), c.telephone),
            email                   = COALESCE(NULLIF(gr.email, ''), c.email),
            site_web                = COALESCE(NULLIF(gr.website, ''), c.site_web),
            reviews_enriched_status = 'done',
            reviews_enriched_at     = gr.scraped_at
        FROM google_reviews gr
        WHERE c.siret = gr.siret
          AND (c.reviews_enriched_at IS NULL OR c.reviews_enriched_at < gr.scraped_at);
    """)
    updated = cur.rowcount
    conn.commit()
    print(f"   ✓ {updated:,} entreprises mises à jour")

    # 3. Stats finales
    print("\n[3/3] Stats finales…")
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE reviews_enriched_status IS NOT NULL) AS enrichies,
            COUNT(*) FILTER (WHERE google_rating IS NOT NULL) AS avec_note,
            COUNT(*) FILTER (WHERE reviews_enriched_at > NOW() - INTERVAL '1 hour') AS derniere_heure,
            COUNT(*) FILTER (WHERE reviews_enriched_at > NOW() - INTERVAL '24 hours') AS dernier_24h
        FROM companies
        WHERE metier_principal IS NOT NULL;
    """)
    enrichies, avec_note, h1, h24 = cur.fetchone()
    print(f"   ✓ Total enrichies      : {enrichies:,}")
    print(f"   ✓ Avec note Google     : {avec_note:,}")
    print(f"   ✓ Dans la dernière h   : {h1:,}")
    print(f"   ✓ Dans les 24h         : {h24:,}")

    print("\n" + "=" * 70)
    print("  ✅ Synchronisation terminée")
    print("=" * 70)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
