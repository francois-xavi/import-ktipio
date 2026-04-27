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

    Workflow exact (basé sur l'inspection de https://www.qualibat.com/annuaire-entreprises-qualifiees) :
      1. Choisir "Vous êtes un Professionnel" (radio)
      2. Ouvrir l'accordéon "RECHERCHE DIRECTE PAR SIRET..."
      3. Saisir SIRET dans le champ RECHERCHE
      4. Cliquer "RECHERCHER"
      5. Résultats : cliquer "Voir la fiche →"
      6. Page détail : extraire qualifications format "XXXX - description"
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
        page.wait_for_timeout(1000)

        # 2. Accepter les cookies si présent (Tarteaucitron / Didomi)
        for cookie_sel in [
            "#tarteaucitronPersonalize2",
            "#tarteaucitronAllAllowed",
            "#didomi-notice-agree-button",
            "button:has-text('Tout accepter')",
            "button:has-text('J\\'accepte')",
            "button:has-text('Accepter')",
            ".tarteaucitronAllow",
        ]:
            try:
                page.click(cookie_sel, timeout=1500)
                page.wait_for_timeout(400)
                break
            except Exception:
                continue

        # 3. Choisir "Vous êtes un Professionnel" (radio button)
        prof_clicked = False
        for prof_sel in [
            "input[type='radio'][value='professional']",
            "label:has-text('Vous êtes un Professionnel')",
            "label:has-text('Professionnel')",
            "div:has-text('Vous êtes un Professionnel') input[type='radio']",
        ]:
            try:
                page.click(prof_sel, timeout=2000)
                prof_clicked = True
                page.wait_for_timeout(800)
                break
            except Exception:
                continue

        if not prof_clicked:
            log.debug("  [Qualibat] Bouton Professionnel non cliqué (peut-être déjà sélectionné)")

        # 4. Ouvrir l'accordéon "RECHERCHE DIRECTE PAR SIRET..."
        # L'accordéon a probablement un bouton ou un lien cliquable
        for acc_sel in [
            "button:has-text('RECHERCHE DIRECTE')",
            "div:has-text('RECHERCHE DIRECTE PAR SIRET') >> nth=0",
            "[class*='accordion']:has-text('RECHERCHE DIRECTE')",
            "h2:has-text('RECHERCHE DIRECTE')",
            "h3:has-text('RECHERCHE DIRECTE')",
        ]:
            try:
                el = page.query_selector(acc_sel)
                if el:
                    el.click(timeout=2000)
                    page.wait_for_timeout(600)
                    break
            except Exception:
                continue

        # 5. Trouver le champ "RECHERCHE" et saisir le SIRET
        # Le champ est visible une fois l'accordéon ouvert
        search_input = None
        for sel in [
            "input[name*='search']",
            "input[name*='recherche']",
            "input[name*='siret']",
            "input[placeholder*='SIRET']",
            "input[placeholder*='entreprise']",
            "input[type='search']:visible",
            "form input[type='text']:visible",
            # Fallback : premier input texte visible dans le formulaire
            "input[type='text']:visible",
        ]:
            try:
                els = page.query_selector_all(sel)
                for el in els:
                    if el.is_visible():
                        search_input = el
                        break
                if search_input:
                    break
            except Exception:
                continue

        if not search_input:
            log.warning(f"  [Qualibat] Champ recherche introuvable")
            return result

        search_input.fill(siret)
        page.wait_for_timeout(400)

        # 6. Cliquer sur "RECHERCHER"
        submitted = False
        for btn_sel in [
            "button:has-text('RECHERCHER')",
            "button:has-text('Rechercher')",
            "button[type='submit']:visible",
            "input[type='submit']:visible",
            "form button:visible",
        ]:
            try:
                page.click(btn_sel, timeout=2500)
                submitted = True
                break
            except Exception:
                continue

        if not submitted:
            try:
                search_input.press("Enter")
                submitted = True
            except Exception:
                pass

        if not submitted:
            log.warning(f"  [Qualibat] Impossible de soumettre la recherche")
            return result

        # 7. Attendre les résultats
        page.wait_for_timeout(2500)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        # 8. Vérifier s'il y a des résultats : on cherche "X ARTISAN(S) CORRESPONDENT" ou similaire
        body_text = ""
        try:
            body_text = page.inner_text("body")
        except Exception:
            return result

        # Si on voit "Aucun" dans la zone résultats → 0 résultat
        if re.search(r"aucun\s*(résultat|artisan|entreprise)", body_text, re.IGNORECASE):
            log.info(f"  [Qualibat] ✗ Aucun résultat pour {siret}")
            return result

        # Chercher le pattern "X ARTISAN(S) CORRESPONDENT"
        match_count = re.search(r"(\d+)\s+ARTISAN", body_text, re.IGNORECASE)
        if match_count:
            log.info(f"  [Qualibat] {match_count.group(1)} résultat(s) trouvé(s)")

        # 9. Cliquer sur "Voir la fiche →" pour aller au détail
        clicked_fiche = False
        for fiche_sel in [
            "a:has-text('Voir la fiche')",
            "a:has-text('voir la fiche')",
            "a[href*='fiche']:visible",
            "a[href*='entreprise']:visible",
        ]:
            try:
                page.click(fiche_sel, timeout=2500)
                clicked_fiche = True
                page.wait_for_timeout(2000)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass
                break
            except Exception:
                continue

        if not clicked_fiche:
            log.info(f"  [Qualibat] Impossible d'ouvrir la fiche détail (peut-être 0 résultat)")
            # On peut quand même essayer d'extraire les qualifications de la page de résultats
            # mais en général il faut cliquer sur la fiche
            return result

        # 10. Sur la page détail : extraire le nom de l'entreprise (h1 ou h2)
        try:
            full_text = page.inner_text("body")
        except Exception:
            full_text = body_text

        for name_sel in ["h1", "h2", ".company-name", "[class*='entreprise-name']"]:
            try:
                el = page.query_selector(name_sel)
                if el:
                    txt = el.inner_text().strip()
                    # Filtrer les titres génériques
                    if txt and len(txt) > 3 and not any(
                        k in txt.lower() for k in ["recherche", "annuaire", "trouver", "qualibat"]
                    ):
                        result["place_name"] = txt
                        break
            except Exception:
                continue

        # 11. Extraire l'adresse (souvent juste après le nom)
        addr_match = re.search(
            r"(\d+\s+[A-Z][A-Za-zéèàâ\s\-]+?)\s*[-–]\s*(\d{5}\s+[A-Z][A-Za-zéèàâ\s\-]+)",
            full_text
        )
        if addr_match:
            result["address"] = f"{addr_match.group(1).strip()} - {addr_match.group(2).strip()}"

        # 12. Extraire les qualifications
        # Format observé sur la fiche détail :
        #   2112 - Maçonnerie et ouvrage en béton armé (Technicité confirmée)
        #   2142 - Réparation en maçonnerie et en béton armé ()
        qualifications_found = []

        # Pattern principal : 4 chiffres - description (avec parenthèses optionnelles)
        # Plus strict : doit commencer en début de ligne ou après une nouvelle ligne
        qual_pattern = re.compile(
            r"(?:^|\n)\s*(\d{4})\s*[-–]\s*([^\n]{5,200})",
            re.MULTILINE
        )

        for match in qual_pattern.finditer(full_text):
            code = match.group(1)
            label = match.group(2).strip()
            # Nettoyer les parenthèses vides à la fin
            label = re.sub(r"\s*\(\s*\)\s*$", "", label)
            # Filtrer les codes hors plage Qualibat (1000-9999)
            if 1000 <= int(code) <= 9999:
                qualif_str = f"{code} - {label}"
                if qualif_str not in qualifications_found:
                    qualifications_found.append(qualif_str)

        # Vérifier aussi le compteur "X QUALIFICATION(S)"
        match_qual_count = re.search(r"(\d+)\s+QUALIFICATION", full_text, re.IGNORECASE)
        expected_count = int(match_qual_count.group(1)) if match_qual_count else 0

        # 13. Résultat final
        if qualifications_found:
            result["is_qualibat"] = True
            result["nb_qualifications"] = len(qualifications_found)
            result["qualifications"] = "; ".join(qualifications_found[:30])
            log.info(f"  [Qualibat] ✓ {result['nb_qualifications']} qualif(s) "
                     f"(attendu: {expected_count}) : "
                     f"{result['qualifications'][:150]}{'...' if len(result['qualifications']) > 150 else ''}")
        elif expected_count > 0:
            # Le compteur indique des qualifs mais on n'a pas réussi à les parser
            log.warning(f"  [Qualibat] ⚠ {expected_count} qualif(s) annoncée(s) "
                        f"mais aucune extraite (sélecteur à corriger ?)")
            # Marquer is_qualibat = true quand même puisque le compteur le dit
            result["is_qualibat"] = True
            result["nb_qualifications"] = expected_count
            result["qualifications"] = f"({expected_count} qualifications non parsées)"
        else:
            log.info(f"  [Qualibat] ✗ Pas de qualifications détectées")

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
