#!/usr/bin/env python3
"""Croisement exact verified_at x is_qualibat + requête worker exacte."""
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

def q(label, sql):
    cur.execute(sql)
    print(f"  {label:<60} : {cur.fetchone()[0]:,}")

print("=" * 80)
print("  REQUÊTE EXACTE DU WORKER (count_pending_qualibat) :")
q("→ ce que le worker voit comme 'à faire'", """
    SELECT COUNT(*) FROM companies
    WHERE metier_principal IS NOT NULL
      AND (is_qualibat IS NULL OR is_qualibat = false)
      AND (qualibat_verified_at IS NULL OR qualibat_verified_at < NOW() - INTERVAL '90 days');
""")
print("-" * 80)
print("  CROISEMENT des 39 701 NON VÉRIFIÉES (verified_at IS NULL) :")
q("  dont is_qualibat = TRUE  (exclues par le worker)", """
    SELECT COUNT(*) FROM companies WHERE qualibat_verified_at IS NULL AND is_qualibat = TRUE;
""")
q("  dont is_qualibat = FALSE (le worker devrait les prendre)", """
    SELECT COUNT(*) FROM companies WHERE qualibat_verified_at IS NULL AND is_qualibat = FALSE;
""")
print("-" * 80)
print("  Parmi les is_qualibat=TRUE non vérifiées : ont-elles des qualifs détaillées ?")
q("  is_qualibat=TRUE, verified_at NULL, nb_qualifications renseigné", """
    SELECT COUNT(*) FROM companies
    WHERE qualibat_verified_at IS NULL AND is_qualibat = TRUE
      AND nb_qualifications_qualibat IS NOT NULL AND nb_qualifications_qualibat > 0;
""")
q("  is_qualibat=TRUE, verified_at NULL, SANS détail de qualif", """
    SELECT COUNT(*) FROM companies
    WHERE qualibat_verified_at IS NULL AND is_qualibat = TRUE
      AND (nb_qualifications_qualibat IS NULL OR nb_qualifications_qualibat = 0);
""")

cur.close()
conn.close()
