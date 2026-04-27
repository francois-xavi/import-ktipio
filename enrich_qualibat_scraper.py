#!/usr/bin/env python3
"""
Scraper Qualibat — qualifications classiques (non-RGE)

Scrape l'annuaire public Qualibat (https://www.qualibat.com/annuaire-entreprises-qualifiees)
pour récupérer les qualifications Qualibat classiques absentes du fichier RGE de l'ADEME.

Usage:
    # Test sur un SIRET unique (dry-run)
    python enrich_qualibat_scraper.py --siret 78005347600021 --dry-run

    # Test sur 10 entreprises (dry-run)
    python enrich_qualibat_scraper.py --limit 10 --dry-run

    # Production : 1 worker
    python enrich_qualibat_scraper.py --batch-size 50 --delay 5

    # Multi-workers (lancer plusieurs avec offsets différents)
    python enrich_qualibat_scraper.py --offset 0 --batch-size 50 --delay 5
    python enrich_qualibat_scraper.py --offset 120000 --batch-size 50 --delay 5
"""

import os
import sys
import time
import logging
import argparse
import asyncio
import re
from datetime import datetime, timezone

# Asyncio fix Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import nest_asyncio
nest_asyncio.apply()

import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright, Page


# ─── CONFIG ───────────────────────────────────────────────────────────────────

DB_URL = os.getenv("NEON_DATABASE_URL")
if not DB_URL:
    print("❌ ERROR: NEON_DATABASE_URL non définie")
    sys.exit(1)

QUALIBAT_URL = "https://www.qualibat.com/annuaire-entreprises-qualifiees"
PAGE_TIMEOUT = 15000  # ms (Qualibat est plus lent que Maps)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("qualibat-scraper")


# ─── DB ───────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DB_URL, connect_timeout=15)


def ensure_db_connected(conn):
    """Vérifie la connexion DB et reconnecte si fermée."""
    try:
        if conn and not conn.closed:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
    except Exception:
        log.warning("  [DB] Connexion fermée, reconnexion...")
    try:
        if conn:
            conn.close()
    except Exception:
        pass
    try:
        return get_conn()
    except Exception as e:
        log.error(f"  ❌ Reconnexion échouée: {e}")
        return None


def fetch_pending_qualibat(conn, batch_size: int, offset: int) -> list[dict]:
    """
    Récupère les entreprises BTP non encore vérifiées Qualibat.
    Priorité aux entreprises déjà enrichies (reviews_enriched_status = 'done').
    """
    query = """
        SELECT siret, raison_sociale, ville
        FROM companies
        WHERE metier_principal IS NOT NULL
          AND (is_qualibat IS NULL OR is_qualibat = false)
          AND (qualibat_verified_at IS NULL
               OR qualibat_verified_at < NOW() - INTERVAL '90 days')
        ORDER BY
          CASE
            WHEN reviews_enriched_status = 'done' THEN 0
            WHEN last_enriched_at IS NOT NULL THEN 1
            ELSE 2
          END,
          siret
        LIMIT %s OFFSET %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (batch_size, offset))
        return [dict(r) for r in cur.fetchall()]


def fetch_one_siret(conn, siret: str) -> dict | None:
    """Récupère une entreprise spécifique par SIRET (pour tests)."""
    query = """
        SELECT siret, raison_sociale, ville
        FROM companies
        WHERE siret = %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (siret,))
        row = cur.fetchone()
        return dict(row) if row else None


def count_pending_qualibat(conn) -> int:
    """Compte les entreprises BTP restantes à scraper."""
    query = """
        SELECT COUNT(*)
        FROM companies
        WHERE metier_principal IS NOT NULL
          AND (is_qualibat IS NULL OR is_qualibat = false)
          AND (qualibat_verified_at IS NULL
               OR qualibat_verified_at < NOW() - INTERVAL '90 days');
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def update_qualibat_db(conn, siret: str, result: dict, dry_run: bool = False) -> bool:
    """Met à jour la DB avec le résultat du scraping Qualibat."""
    if dry_run:
        log.info(f"  [DRY-RUN] UPDATE siret={siret} is_qualibat={result['is_qualibat']} "
                 f"nb={result['nb_qualifications']} qualifs={result['qualifications'][:80] if result['qualifications'] else '—'}")
        return True

    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE companies
                SET
                    is_qualibat = %s,
                    nb_qualifications_qualibat = %s,
                    qualibat_verified_at = NOW(),
                    rge_qualifications = COALESCE(NULLIF(%s, ''), rge_qualifications)
                WHERE siret = %s;
            """, (
                bool(result["is_qualibat"]),
                int(result["nb_qualifications"]),
                result["qualifications"] or "",
                siret,
            ))
            conn.commit()
        return True
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        log.error(f"  ❌ UPDATE échoué ({siret}): {e}")
        return False


# ─── PLAYWRIGHT ───────────────────────────────────────────────────────────────

def ensure_browser_valid(pw, browser, ctx, page, headed: bool = False):
    """Recrée le browser si crashé."""
    try:
        _ = page.title()
        return browser, ctx, page
    except Exception:
        log.warning("  [Browser] Crash — recréation...")
        try:
            browser.close()
        except Exception:
            pass
        browser = pw.chromium.launch(
            headless=not headed,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-first-run",
                "--disable-extensions",
                "--lang=fr-FR",
                "--disable-blink-features=AutomationControlled",
                "--js-flags=--max-old-space-size=256",
                "--renderer-process-limit=1",
            ],
        )
        ctx = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()
        log.info("  [Browser] ✓ Recréé")
        return browser, ctx, page


# ─── SCRAPING QUALIBAT ────────────────────────────────────────────────────────

def scrape_qualibat(page: Page, siret: str, name: str = "", city: str = "") -> dict:
    """
    Scrape l'annuaire Qualibat pour un SIRET.
    Retourne {is_qualibat, nb_qualifications, qualifications, place_name, address}.
    """
    result = {
        "is_qualibat": False,
        "nb_qualifications": 0,
        "qualifications": "",
        "place_name": "",
        "address": "",
    }

    try:
        # 1. Charger l'annuaire
        page.goto(QUALIBAT_URL, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        page.wait_for_timeout(800)

        # 2. Accepter les cookies si présent
        for cookie_sel in [
            "#didomi-notice-agree-button",
            "button:has-text('J\\'accepte')",
            "button:has-text('Accepter')",
            "[id*='accept'][id*='cookie']",
        ]:
            try:
                page.click(cookie_sel, timeout=1500)
                page.wait_for_timeout(300)
                break
            except Exception:
                continue

        # 3. Choisir "Professionnel" si une popup s'affiche
        for prof_sel in [
            "input[value='professional']",
            "label:has-text('Professionnel')",
            "button:has-text('Professionnel')",
        ]:
            try:
                page.click(prof_sel, timeout=1500)
                page.wait_for_timeout(500)
                break
            except Exception:
                continue

        # 4. Trouver le champ "recherche" et saisir le SIRET
        search_input = None
        for sel in [
            "input[name*='search']",
            "input[name*='recherche']",
            "input[placeholder*='SIRET']",
            "input[placeholder*='entreprise']",
            "input[type='search']",
            "input.search-input",
            "form input[type='text']:visible",
        ]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    search_input = sel
                    break
            except Exception:
                continue

        if not search_input:
            log.warning(f"  [Qualibat] Champ recherche introuvable")
            return result

        page.fill(search_input, siret)
        page.wait_for_timeout(400)

        # 5. Soumettre le formulaire
        submitted = False
        for btn_sel in [
            "button:has-text('Rechercher')",
            "button[type='submit']",
            "input[type='submit']",
            "form button:visible",
        ]:
            try:
                page.click(btn_sel, timeout=2000)
                submitted = True
                break
            except Exception:
                continue

        if not submitted:
            # Fallback : appuyer sur Entrée
            try:
                page.press(search_input, "Enter")
                submitted = True
            except Exception:
                pass

        if not submitted:
            log.warning(f"  [Qualibat] Impossible de soumettre")
            return result

        # 6. Attendre les résultats (page de résultats)
        page.wait_for_timeout(2500)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        # 7. Vérifier s'il y a des résultats
        no_result_indicators = [
            "Aucun résultat",
            "aucune entreprise",
            "pas d'entreprise",
            "no result",
        ]
        body_text = ""
        try:
            body_text = page.inner_text("body").lower()
        except Exception:
            pass

        if any(ind.lower() in body_text for ind in no_result_indicators):
            log.info(f"  [Qualibat] ✗ Aucun résultat pour {siret}")
            return result

        # 8. Extraire le nom de l'entreprise (premier résultat)
        for name_sel in [
            "h1",
            "h2.company-name",
            ".result-item h2",
            ".entreprise-name",
            "[class*='entreprise'] h2",
            "[class*='company'] h2",
        ]:
            try:
                el = page.query_selector(name_sel)
                if el:
                    txt = el.inner_text().strip()
                    if txt and len(txt) > 2:
                        result["place_name"] = txt
                        break
            except Exception:
                continue

        # 9. Extraire l'adresse
        for addr_sel in [
            ".address",
            ".adresse",
            "[class*='address']",
            "[class*='adresse']",
        ]:
            try:
                el = page.query_selector(addr_sel)
                if el:
                    result["address"] = el.inner_text().strip()
                    break
            except Exception:
                continue

        # 10. Extraire la liste des qualifications
        # Approche 1 : chercher des éléments avec des codes Qualibat (4 chiffres + lettre, ex: "1311 BTP")
        qualifications_found = set()

        # Scanner tout le texte de la page pour des codes Qualibat
        qualibat_code_pattern = re.compile(r"\b(\d{4})\s*[-–]?\s*([A-Z][A-Za-zéèà\s\-,/'.]{5,80})", re.MULTILINE)
        try:
            full_text = page.inner_text("body")
            for match in qualibat_code_pattern.finditer(full_text):
                code = match.group(1)
                label = match.group(2).strip()
                if 1000 <= int(code) <= 9999 and len(label) > 5:
                    qualifications_found.add(f"{code} {label}")
        except Exception:
            pass

        # Approche 2 : sélecteurs spécifiques aux qualifications
        for qual_sel in [
            ".qualification-item",
            ".qualifications li",
            "[class*='qualification'] li",
            ".certification-item",
            ".qualification",
        ]:
            try:
                els = page.query_selector_all(qual_sel)
                for el in els:
                    txt = el.inner_text().strip()
                    if txt and 5 < len(txt) < 200:
                        qualifications_found.add(txt)
            except Exception:
                continue

        # 11. Déterminer le résultat final
        if qualifications_found:
            result["is_qualibat"] = True
            result["nb_qualifications"] = len(qualifications_found)
            result["qualifications"] = "; ".join(sorted(qualifications_found)[:30])  # max 30
            log.info(f"  [Qualibat] ✓ {result['nb_qualifications']} qualifs : "
                     f"{result['qualifications'][:120]}...")
        else:
            # Page chargée mais pas de qualifications détectables
            # → soit pas dans Qualibat, soit sélecteurs inadaptés
            log.info(f"  [Qualibat] ✗ Pas de qualifications détectées (page chargée)")

    except Exception as e:
        if "closed" in str(e).lower():
            log.warning(f"  [Qualibat] Page fermée (sera recréée)")
        else:
            log.warning(f"  [Qualibat] Erreur: {e}")

    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Scraper Qualibat — qualifications classiques")
    p.add_argument("--siret", type=str, default=None, help="Test sur un SIRET unique")
    p.add_argument("--limit", type=int, default=None, help="Max entreprises à traiter")
    p.add_argument("--offset", type=int, default=0, help="Offset SQL (pour multi-workers)")
    p.add_argument("--batch-size", type=int, default=50, help="Entreprises par batch")
    p.add_argument("--delay", type=int, default=5, help="Secondes entre entreprises")
    p.add_argument("--dry-run", action="store_true", help="Pas d'écriture en DB")
    p.add_argument("--headed", action="store_true", help="Navigateur visible (debug)")
    return p.parse_args()


def main():
    args = parse_args()

    print("═" * 70)
    print("  KTIPIO — Scraper Qualibat (qualifications classiques)")
    print(f"  Mode      : {'DRY-RUN' if args.dry_run else 'Production'}")
    print(f"  Source    : {QUALIBAT_URL}")
    print(f"  Délai     : {args.delay}s entre entreprises")
    print(f"  Offset    : {args.offset}")
    print("═" * 70)

    # Connexion DB
    log.info("Connexion à Neon PostgreSQL…")
    conn = get_conn()
    log.info("  ✓ Connecté")

    # Compter le total
    if not args.siret:
        total_pending = count_pending_qualibat(conn)
        log.info(f"  📋 {total_pending:,} entreprises BTP à vérifier sur Qualibat")
    else:
        total_pending = 1

    total_processed = 0
    total_qualibat_found = 0
    current_offset = args.offset

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=not args.headed,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-first-run",
                "--disable-extensions",
                "--lang=fr-FR",
                "--disable-blink-features=AutomationControlled",
                "--js-flags=--max-old-space-size=256",
                "--renderer-process-limit=1",
            ],
        )
        ctx = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = ctx.new_page()

        try:
            # Mode SIRET unique (test)
            if args.siret:
                conn = ensure_db_connected(conn)
                company = fetch_one_siret(conn, args.siret)
                if not company:
                    log.error(f"  ❌ SIRET {args.siret} introuvable en DB")
                    sys.exit(1)

                log.info(f"\n  [TEST] {company.get('raison_sociale')} — {company.get('ville')}")
                result = scrape_qualibat(page, args.siret,
                                         company.get("raison_sociale", ""),
                                         company.get("ville", ""))

                log.info(f"\n  📊 RÉSULTAT FINAL :")
                log.info(f"     is_qualibat       : {result['is_qualibat']}")
                log.info(f"     nb_qualifications : {result['nb_qualifications']}")
                log.info(f"     qualifications    : {result['qualifications']}")
                log.info(f"     place_name        : {result['place_name']}")
                log.info(f"     address           : {result['address']}")

                if not args.dry_run:
                    update_qualibat_db(conn, args.siret, result, dry_run=False)
                else:
                    update_qualibat_db(conn, args.siret, result, dry_run=True)

                return

            # Mode batch
            while True:
                if args.limit and total_processed >= args.limit:
                    break

                batch_size = args.batch_size
                if args.limit:
                    batch_size = min(batch_size, args.limit - total_processed)

                conn = ensure_db_connected(conn)
                if not conn:
                    log.error("  ❌ Pas de DB, arrêt.")
                    break

                companies = fetch_pending_qualibat(conn, batch_size, current_offset)
                if not companies:
                    log.info("✅ Plus d'entreprises à traiter.")
                    break

                log.info(f"\n{'─' * 55}")
                log.info(f"  OFFSET {current_offset} — {len(companies)} entreprises")
                log.info(f"  Progression: {total_processed:,}/{total_pending:,}  "
                         f"(Qualibat trouvés: {total_qualibat_found:,})")
                log.info(f"{'─' * 55}")

                for i, company in enumerate(companies):
                    if args.limit and total_processed >= args.limit:
                        break

                    browser, ctx, page = ensure_browser_valid(pw, browser, ctx, page, args.headed)

                    siret = company.get("siret", "")
                    name = company.get("raison_sociale", "") or ""
                    city = company.get("ville", "") or ""

                    log.info(f"\n  [{total_processed + 1}/{total_pending:,}] {name} — {city} ({siret})")

                    result = scrape_qualibat(page, siret, name, city)

                    conn = ensure_db_connected(conn)
                    if not conn:
                        log.error("  ❌ DB perdue, arrêt.")
                        break

                    if update_qualibat_db(conn, siret, result, dry_run=args.dry_run):
                        total_processed += 1
                        if result["is_qualibat"]:
                            total_qualibat_found += 1

                    if i < len(companies) - 1:
                        log.info(f"  ⏳ {args.delay}s…")
                        time.sleep(args.delay)

                if len(companies) < batch_size:
                    log.info("✅ Dernière page.")
                    break

                current_offset += len(companies)

        except KeyboardInterrupt:
            log.info("\n⛔ Arrêt (Ctrl+C)")
        finally:
            try:
                browser.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    print("═" * 70)
    print(f"  ✅ Terminé")
    print(f"     Traités       : {total_processed:,}")
    print(f"     Qualibat trouvés : {total_qualibat_found:,}")
    print(f"     Reprendre     : --offset {current_offset}")
    print("═" * 70)


if __name__ == "__main__":
    main()
