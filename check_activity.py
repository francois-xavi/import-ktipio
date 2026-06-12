#!/usr/bin/env python3
"""Diagnostic : quand les scrapers se sont-ils arrêtés ? Histogramme par heure."""
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
print(f"Heure actuelle de la base (UTC) : {cur.fetchone()[0]}\n")

print("=" * 64)
print("  🌐 GOOGLE — activité par heure (dernières 48h)")
print("=" * 64)
cur.execute("""
    SELECT date_trunc('hour', scraped_at) AS h, COUNT(*)
    FROM google_reviews
    WHERE scraped_at > NOW() - INTERVAL '48 hours'
    GROUP BY h ORDER BY h DESC LIMIT 12;
""")
rows = cur.fetchall()
if not rows:
    print("  (aucune activité sur 48h)")
for h, c in rows:
    print(f"  {h}  →  {c:,}")

print("\n" + "=" * 64)
print("  🏅 QUALIBAT — activité par heure (dernières 48h)")
print("=" * 64)
cur.execute("""
    SELECT date_trunc('hour', qualibat_verified_at) AS h, COUNT(*)
    FROM companies
    WHERE qualibat_verified_at > NOW() - INTERVAL '48 hours'
    GROUP BY h ORDER BY h DESC LIMIT 12;
""")
rows = cur.fetchall()
if not rows:
    print("  (aucune activité sur 48h)")
for h, c in rows:
    print(f"  {h}  →  {c:,}")

cur.close()
conn.close()
