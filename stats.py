#!/usr/bin/env python3
"""
Script de statistiques : bilan complet de l'enrichissement BTP.

Affiche :
  - Total entreprises BTP
  - Entreprises traitées par Google Reviews (téléphone, email, site)
  - Entreprises traitées par Qualibat (qualifications)
  - Restant à traiter
  - Vitesse récente (1h, 24h)
  - Estimation temps restant

Usage :
  docker compose run --rm --entrypoint python enrichment-worker stats.py
"""

import os
import sys
import psycopg2

DB_URL = os.getenv("NEON_DATABASE_URL")
if not DB_URL:
    print("❌ ERROR: NEON_DATABASE_URL non définie")
    sys.exit(1)


def format_num(n):
    """Format avec séparateur de milliers."""
    return f"{n:,}".replace(",", " ")


def format_percent(part, total):
    if total == 0:
        return "0.00%"
    return f"{100.0 * part / total:.2f}%"


def main():
    print("=" * 70)
    print("  📊 BILAN ENRICHISSEMENT BTP — KTIPIO")
    print("=" * 70)

    conn = psycopg2.connect(DB_URL, connect_timeout=15)
    cur = conn.cursor()

    # ── Requête 1 : Stats globales ─────────────────────────────────────────────
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE reviews_enriched_status IS NOT NULL) AS google_done,
            COUNT(*) FILTER (WHERE google_rating IS NOT NULL) AS avec_note,
            COUNT(*) FILTER (WHERE telephone IS NOT NULL AND telephone != '') AS avec_tel,
            COUNT(*) FILTER (WHERE email IS NOT NULL AND email != '') AS avec_email,
            COUNT(*) FILTER (WHERE site_web IS NOT NULL AND site_web != '') AS avec_site,
            COUNT(*) FILTER (WHERE qualibat_verified_at IS NOT NULL) AS qualibat_done,
            COUNT(*) FILTER (WHERE is_qualibat = true) AS qualibat_yes,
            COUNT(*) FILTER (WHERE is_rge = true) AS rge_yes
        FROM companies
        WHERE metier_principal IS NOT NULL;
    """)
    (total, google_done, avec_note, avec_tel, avec_email, avec_site,
     qualibat_done, qualibat_yes, rge_yes) = cur.fetchone()

    google_todo = total - google_done
    qualibat_todo = total - qualibat_done

    print()
    print(f"  📦 Total entreprises BTP        : {format_num(total)}")
    print()

    # ── Google Reviews ─────────────────────────────────────────────────────────
    print("  🟦 GOOGLE REVIEWS (Maps + Pages Jaunes + Web)")
    print(f"     ✓ Traitées              : {format_num(google_done):>12s}  ({format_percent(google_done, total)})")
    print(f"     ↳ Avec note Google      : {format_num(avec_note):>12s}  ({format_percent(avec_note, total)})")
    print(f"     ↳ Avec téléphone        : {format_num(avec_tel):>12s}  ({format_percent(avec_tel, total)})")
    print(f"     ↳ Avec email            : {format_num(avec_email):>12s}  ({format_percent(avec_email, total)})")
    print(f"     ↳ Avec site web         : {format_num(avec_site):>12s}  ({format_percent(avec_site, total)})")
    print(f"     ⏳ Restantes            : {format_num(google_todo):>12s}  ({format_percent(google_todo, total)})")
    print()

    # ── Qualibat ──────────────────────────────────────────────────────────────
    print("  🟧 QUALIFICATIONS QUALIBAT")
    print(f"     ✓ Vérifiées             : {format_num(qualibat_done):>12s}  ({format_percent(qualibat_done, total)})")
    print(f"     ↳ Qualibat trouvés      : {format_num(qualibat_yes):>12s}  ({format_percent(qualibat_yes, qualibat_done) if qualibat_done else '—'} des vérifiées)")
    print(f"     ↳ RGE certifiées        : {format_num(rge_yes):>12s}")
    print(f"     ⏳ Restantes            : {format_num(qualibat_todo):>12s}  ({format_percent(qualibat_todo, total)})")
    print()

    # ── Requête 2 : Vitesse récente ────────────────────────────────────────────
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE qualibat_verified_at > NOW() - INTERVAL '1 hour') AS q_1h,
            COUNT(*) FILTER (WHERE qualibat_verified_at > NOW() - INTERVAL '24 hours') AS q_24h,
            COUNT(*) FILTER (WHERE reviews_enriched_at > NOW() - INTERVAL '1 hour') AS g_1h,
            COUNT(*) FILTER (WHERE reviews_enriched_at > NOW() - INTERVAL '24 hours') AS g_24h
        FROM companies
        WHERE metier_principal IS NOT NULL;
    """)
    q_1h, q_24h, g_1h, g_24h = cur.fetchone()

    print("  🚀 VITESSE RÉCENTE")
    print(f"     Qualibat : {format_num(q_1h)} dans la dernière heure  |  {format_num(q_24h)} dans les 24h")
    print(f"     Google   : {format_num(g_1h)} dans la dernière heure  |  {format_num(g_24h)} dans les 24h")
    print()

    # ── Estimations ─────────────────────────────────────────────────────────────
    print("  ⏱  ESTIMATIONS RESTANTES")
    if q_24h > 0:
        jours_q = qualibat_todo / q_24h
        print(f"     Qualibat : ~{jours_q:.1f} jours restants (à la vitesse actuelle)")
    else:
        print(f"     Qualibat : pas de données récentes")

    if g_24h > 0:
        jours_g = google_todo / g_24h
        print(f"     Google   : ~{jours_g:.1f} jours restants (à la vitesse actuelle)")
    else:
        print(f"     Google   : pas en cours d'enrichissement")
    print()

    # ── Requête 3 : Top qualifications ──────────────────────────────────────────
    if qualibat_yes > 0:
        cur.execute("""
            SELECT
                unnest(string_to_array(rge_qualifications, '; ')) AS qualif,
                COUNT(*) AS nb
            FROM companies
            WHERE is_qualibat = true
              AND rge_qualifications IS NOT NULL
              AND rge_qualifications != ''
            GROUP BY qualif
            ORDER BY nb DESC
            LIMIT 10;
        """)
        rows = cur.fetchall()
        if rows:
            print("  🏆 TOP 10 QUALIFICATIONS QUALIBAT")
            for qualif, nb in rows:
                qualif_short = qualif[:60] + "..." if len(qualif) > 60 else qualif
                print(f"     {format_num(nb):>6s}  {qualif_short}")
            print()

    print("=" * 70)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
