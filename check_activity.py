#!/usr/bin/env python3
"""Vérifie la FRAÎCHEUR des colonnes companies utilisées par le dashboard."""
import os
import psycopg2

url = os.getenv("NEON_DATABASE_URL")
if not url:
    with open(".env", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("NEON_DATABASE_URL"):
                url = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

conn = psycopg2.connect(url, connect_timeout=30)
cur = conn.cursor()

cur.execute("SELECT NOW();")
print(f"NOW() base (UTC) : {cur.fetchone()[0]}\n")

print("=" * 70)
print("  GOOGLE — companies.reviews_enriched_at (source dashboard)")
print("=" * 70)
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE reviews_enriched_at > NOW() - INTERVAL '5 minutes') AS m5,
        COUNT(*) FILTER (WHERE reviews_enriched_at > NOW() - INTERVAL '1 hour')     AS h1,
        MAX(reviews_enriched_at) AS dernier
    FROM companies;
""")
m5, h1, last = cur.fetchone()
print(f"  5 dern. min : {m5:,}   |   1 h : {h1:,}   |   dernier : {last}")

print("\n" + "=" * 70)
print("  QUALIBAT — companies.qualibat_verified_at (source dashboard)")
print("=" * 70)
cur.execute("""
    SELECT
        COUNT(*) FILTER (WHERE qualibat_verified_at > NOW() - INTERVAL '5 minutes') AS m5,
        COUNT(*) FILTER (WHERE qualibat_verified_at > NOW() - INTERVAL '1 hour')     AS h1,
        MAX(qualibat_verified_at) AS dernier
    FROM companies;
""")
m5, h1, last = cur.fetchone()
print(f"  5 dern. min : {m5:,}   |   1 h : {h1:,}   |   dernier : {last}")

cur.close()
conn.close()
