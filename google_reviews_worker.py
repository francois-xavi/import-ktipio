#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  KTIPIO — ENRICHISSEMENT CONTACTS BTP (v3)                                  ║
║                                                                             ║
║  Architecture cascade (ordre de priorité) :                                 ║
║    1. API gouv.fr en masse (asyncio, 50 concurrent) — gratuit, rapide       ║
║    2. Playwright Google Maps — note + avis + contacts                       ║
║    3. Playwright Pages Jaunes — téléphone + site                            ║
║    4. Scraping profond du site web — navigation dans les menus              ║
║       → cherche les liens "contact", clique dessus, extrait email/tel       ║
║                                                                             ║
║  Objectif : 250 000+ entreprises en 8h                                      ║
║                                                                             ║
║  USAGE:                                                                     ║
║    python enrichissement_v3.py                    # production              ║
║    python enrichissement_v3.py --limit 20         # test                    ║
║    python enrichissement_v3.py --dry-run          # sans POST               ║
║    python enrichissement_v3.py --headed           # voir Chromium           ║
║    python enrichissement_v3.py --skip-playwright  # API gouv seulement      ║
║    python enrichissement_v3.py --page 5           # reprendre page 5        ║
║                                                                             ║
║  INSTALLATION:                                                              ║
║    pip install playwright httpx requests tqdm                               ║
║    playwright install chromium                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import argparse
import asyncio
import json
import logging
import os
import re
import time
import urllib.parse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Optional
from dotenv import load_dotenv

import sys
import httpx
import requests
import nest_asyncio
import psycopg2
import psycopg2.extras

# Charger les variables d'environnement depuis .env (avec chemin absolu)
_script_dir = os.path.dirname(os.path.abspath(__file__))
_env_file = os.path.join(_script_dir, ".env")
if os.path.exists(_env_file):
    load_dotenv(_env_file)
else:
    load_dotenv()  # Fallback: cherche .env dans le répertoire courant
from playwright.sync_api import Page, TimeoutError as PWTimeout, sync_playwright
from tqdm import tqdm

# Fix asyncio sur Windows (évite RuntimeError: cannot be called from a running event loop)
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Allow nested event loops (needed for asyncio.run() inside sync_playwright context)
nest_asyncio.apply()

# ─── Config ────────────────────────────────────────────────────────────────────
DB_URL = os.getenv(
    "NEON_DATABASE_URL",
    os.getenv("DATABASE_URL", "postgresql://user:password@host/dbname?sslmode=require")
)

PLAYWRIGHT_DELAY   = 15     # secondes entre chaque entreprise Playwright
API_CONCURRENT     = 5      # requêtes parallèles vers l'API gouv (réduit pour éviter throttling)
API_BATCH_SIZE     = 500    # entreprises par page
WEBSITE_MAX_LINKS  = 4      # max liens "contact" à visiter par site
PAGE_TIMEOUT       = 12000  # ms timeout Playwright
API_RETRY_DELAY    = 2      # délai en secondes entre les tentatives API
MAPS_NAME_MATCH_THRESHOLD = 0.65  # 65% similarity required for company name match

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  REGEX & FILTRES
# ══════════════════════════════════════════════════════════════════════════════

FR_PHONE = re.compile(
    r"(?:(?:\+|00)33[\s.\-]?|0)[1-9](?:[\s.\-]?\d{2}){4}"
)
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)

# Domaines à rejeter absolument
BAD_EMAIL_DOMAINS = {
    # Plateformes annuaires
    "pagesjaunes", "pagesjaunes.fr", "societe.com", "pappers", "pappers.fr",
    "manageo", "verif.com", "infogreffe", "bodacc", "sirene", "rncs",
    "annuaire-entreprises", "entreprises.gouv", "data.gouv",
    # Hébergeurs génériques
    "gmail", "googlemail", "yahoo", "yahoo.fr", "hotmail", "hotmail.fr",
    "outlook", "live.fr", "live.com", "icloud", "me.com", "msn",
    "orange.fr", "wanadoo.fr", "sfr.fr", "free.fr", "laposte.net",
    "bbox.fr", "numericable", "alice.fr", "club-internet",
    # Techniques / bots
    "sentry", "noreply", "no-reply", "donotreply", "mailer", "postmaster",
    "webmaster", "admin", "support", "contact@google",
    # Constructeurs de sites
    "wix.com", "wordpress", "jimdo", "webflow", "squarespace", "shopify",
    "prestashop", "ovh.com", "ionos", "1and1", "gandi.net",
    # Divers parasites
    "example", "exemple", "test.", "schema", "gstatic", "cloudflare",
    "jquery", "bootstrap", "w3.org", "google.com", "google.fr",
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com",
}

# Mots-clés pour détecter les liens "contact" dans les menus
CONTACT_KEYWORDS = re.compile(
    r"contact|contacter|nous.?écrire|nous.?joindre|devis|coordonn|"
    r"rendez.?vous|renseignement|joindre|atteindre|formulaire|reach|"
    r"get.?in.?touch|write.?to",
    re.IGNORECASE,
)

# Mots-clés pour détecter les liens à ÉVITER (réseaux sociaux, mentions légales…)
SKIP_KEYWORDS = re.compile(
    r"facebook|twitter|instagram|linkedin|youtube|tiktok|pinterest|"
    r"mentions.?légales|politique|rgpd|cgu|cgv|cookies|privacy|legal",
    re.IGNORECASE,
)


def clean_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"[^\d+]", "", raw.strip())
    if len(re.sub(r"\D", "", digits)) < 10:
        return None
    return digits


def is_valid_email(email: str, site_domain: str = "") -> bool:
    """
    Valide un email :
    - Rejette les domaines blacklistés
    - Préfère les emails dont le domaine correspond au site de l'entreprise
    """
    if not email or "@" not in email:
        return False
    domain = email.split("@")[-1].lower()

    # Rejet sur domaine exact ou partiel
    for bad in BAD_EMAIL_DOMAINS:
        if bad in domain:
            return False

    return True


def score_email(email: str, site_domain: str) -> int:
    """Score pour classer les emails : plus haut = meilleur."""
    domain = email.split("@")[-1].lower()
    if site_domain and site_domain in domain:
        return 100  # email du même domaine que le site → parfait
    if any(x in domain for x in ["contact", "info", "bonjour", "hello", "accueil"]):
        return 50
    return 10


def extract_best_contacts(text: str, site_domain: str = "") -> dict:
    """Extrait et classe email + téléphone depuis un texte brut."""
    phones = FR_PHONE.findall(text or "")
    raw_emails = EMAIL_RE.findall(text or "")

    valid_emails = [e for e in raw_emails if is_valid_email(e, site_domain)]
    valid_emails.sort(key=lambda e: score_email(e, site_domain), reverse=True)

    return {
        "phone": clean_phone(phones[0]) if phones else None,
        "email": valid_emails[0] if valid_emails else None,
    }


def get_site_domain(url: str) -> str:
    """Extrait le domaine principal d'une URL."""
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        return domain
    except Exception:
        return ""


def validate_company_name(company_name: str, place_name: str, threshold: float = MAPS_NAME_MATCH_THRESHOLD) -> bool:
    """
    Validate that place_name matches company_name with fuzzy matching.

    Rejects Google Maps results that don't match the searched company name.
    Uses normalized string comparison and fuzzy matching with SequenceMatcher.

    Args:
        company_name: Original company name being searched
        place_name: Name found on Google Maps
        threshold: Minimum similarity ratio (0.0-1.0), default 0.65

    Returns:
        True if names match with sufficient similarity, False otherwise
    """
    if not company_name or not place_name:
        return False

    # Normalize both names for comparison
    company_normalized = company_name.lower().strip()
    place_normalized = place_name.lower().strip()

    # Exact match after normalization
    if company_normalized == place_normalized:
        return True

    # Check if one name is fully contained in the other (account for business type suffixes like SARL, S.A.R.L., etc.)
    if company_normalized in place_normalized or place_normalized in company_normalized:
        return True

    # Fuzzy matching using SequenceMatcher
    similarity = SequenceMatcher(None, company_normalized, place_normalized).ratio()
    return similarity >= threshold


# ══════════════════════════════════════════════════════════════════════════════
#  MODÈLE DE DONNÉES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class EnrichResult:
    siret:        str
    found:        bool
    rating:       Optional[float]
    review_count: int
    rating_text:  str
    place_id:     str
    place_name:   str
    place_url:    str
    phone:        Optional[str]
    website:      Optional[str]
    email:        Optional[str]
    address:      Optional[str]
    category:     Optional[str]
    phone_source: str
    email_source: str
    scraped_at:   str


def empty_result(siret: str) -> EnrichResult:
    return EnrichResult(
        siret=siret, found=False, rating=None, review_count=0,
        rating_text="", place_id="", place_name="", place_url="",
        phone=None, website=None, email=None, address=None, category=None,
        phone_source="", email_source="",
        scraped_at=datetime.now(timezone.utc).isoformat(),
    )


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 1 — API gouv.fr (asyncio, masse)
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_one_gouv(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    siret: str,
    name: str,
    city: str,
) -> dict:
    """Appelle l'API gouv pour une entreprise, retourne website + téléphone."""
    result = {"siret": siret, "website": None, "phone": None, "dirigeant": None}
    queries = [q for q in [siret, f"{name} {city}".strip()] if q]

    async with semaphore:
        for query in queries:
            try:
                r = await client.get(
                    "https://recherche-entreprises.api.gouv.fr/search",
                    params={"q": query, "per_page": 3},
                    timeout=8,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                hits = data.get("results", [])
                if not hits:
                    continue

                hit  = hits[0]
                siege = hit.get("siege") or {}

                site = hit.get("site_internet") or siege.get("site_internet")
                if site and "google" not in site and len(site) > 5:
                    result["website"] = site.rstrip("/")

                # Dirigeant
                dirs = hit.get("dirigeants") or []
                if dirs:
                    d = dirs[0]
                    if d.get("type_dirigeant") == "personne physique":
                        result["dirigeant"] = f"{d.get('prenoms','')} {d.get('nom','')}".strip()
                    else:
                        result["dirigeant"] = d.get("denomination", "")

                if result["website"]:
                    return result

            except Exception:
                continue
        await asyncio.sleep(0.05)
    return result


async def bulk_api_gouv(companies: list[dict], concurrent: int = API_CONCURRENT) -> dict:
    """
    Enrichit toutes les entreprises via l'API gouv en parallèle.
    Retourne un dict { siret: { website, phone, dirigeant } }

    Args:
        companies: List of company dicts to enrich
        concurrent: Number of parallel requests (default: API_CONCURRENT)
    """
    semaphore = asyncio.Semaphore(concurrent)
    results   = {}

    async with httpx.AsyncClient(
        headers={"User-Agent": "ktipio-enricher/3.0"},
        follow_redirects=True,
    ) as client:
        tasks = [
            fetch_one_gouv(
                client, semaphore,
                c.get("siret", ""),
                c.get("name", c.get("nom_entreprise", "")),
                c.get("city", c.get("libelleCommuneEtablissement", "")),
            )
            for c in companies
        ]
        bar = tqdm(total=len(tasks), desc="  API gouv", unit="req", leave=False)
        for coro in asyncio.as_completed(tasks):
            r = await coro
            results[r["siret"]] = r
            bar.update(1)
        bar.close()

    found = sum(1 for v in results.values() if v.get("website"))
    log.info(f"  [API gouv] ✓ {found}/{len(companies)} sites trouvés")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 2 — Google Maps (Playwright)
# ══════════════════════════════════════════════════════════════════════════════

def ensure_page_valid(page: Page, ctx) -> Page:
    """
    Vérifie que la page est valide, sinon crée une nouvelle.
    Utile si le browser/context se ferme pendant le traitement.
    """
    try:
        # Essayer d'accéder à une propriété pour vérifier si c'est valide
        _ = page.title
        return page
    except Exception:
        # Page fermée, créer une nouvelle
        log.warning("  [Page] Recréation (ancienne fermée)")
        return ctx.new_page()


def scrape_google_maps(page: Page, name: str, city: str) -> dict:
    result = {
        "found": False, "rating": None, "review_count": 0,
        "rating_text": "", "place_id": "", "place_name": "",
        "place_url": "", "phone": None, "website": None,
        "address": None, "category": None,
    }
    query = f"{name} {city}".strip()
    if not query:
        return result

    try:
        page.goto(
            f"https://www.google.com/maps/search/{urllib.parse.quote_plus(query)}?hl=fr",
            timeout=PAGE_TIMEOUT, wait_until="domcontentloaded",
        )
        # Cookies
        try:
            page.click(
                "button[aria-label*='Tout accepter'], button[aria-label*='Accept all']",
                timeout=3000,
            )
            page.wait_for_timeout(1000)
        except Exception:
            pass

        # Premier résultat
        try:
            page.wait_for_selector(".hfpxzc", timeout=5000)
            page.click(".hfpxzc", timeout=3000)
            page.wait_for_timeout(1500)
        except Exception:
            pass

        try:
            page.wait_for_selector(".F7nice, .DUwDvf, [data-item-id]", timeout=7000)
        except PWTimeout:
            return result

        # Note
        rating = None
        for sel in [".F7nice span[aria-hidden='true']", ".F7nice", ".Aq14fc"]:
            el = page.query_selector(sel)
            if el:
                m = re.search(r"(\d[,\.]\d)", el.inner_text())
                if m:
                    rating = float(m.group(1).replace(",", "."))
                    break

        # Nombre d'avis
        review_count = 0
        for sel in ["[aria-label*='avis']", "button[jsaction*='review'] span"]:
            el = page.query_selector(sel)
            if el:
                txt = el.inner_text() or el.get_attribute("aria-label") or ""
                m = re.search(r"([\d\s\xa0]+)\s*avis", txt)
                if m:
                    review_count = int(re.sub(r"[\s\xa0]", "", m.group(1)))
                    break

        # Nom
        place_name = ""
        for sel in ["h1.DUwDvf", ".DUwDvf"]:
            el = page.query_selector(sel)
            if el:
                place_name = el.inner_text().strip()
                break

        # Validate that the place name matches the searched company name
        if place_name and not validate_company_name(name, place_name):
            log.info(f"  [Maps] ✗ Name mismatch: searched '{name}' but found '{place_name}'")
            # Reset results as invalid (name doesn't match)
            place_name = ""
            rating = None
            review_count = 0
            rating_text = ""
            place_id = ""

        # Téléphone
        phone = None
        for sel in [
            "[data-item-id='phone:tel:'] span",
            "[aria-label*='Téléphone'] span",
            "button[data-item-id*='phone'] span",
        ]:
            el = page.query_selector(sel)
            if el:
                phone = clean_phone(el.inner_text())
                if phone:
                    break
        if not phone:
            contacts = extract_best_contacts(page.inner_text("body"))
            phone = contacts["phone"]

        # Site web
        website = None
        for sel in [
            "a[data-item-id='authority']",
            "a[aria-label*='Site Web']",
            "a[aria-label*='Website']",
        ]:
            el = page.query_selector(sel)
            if el:
                href = el.get_attribute("href") or ""
                if href and "google" not in href:
                    website = href.rstrip("/")
                    break

        # Adresse
        address = None
        for sel in ["[data-item-id*='address'] span", "[aria-label*='Adresse'] span"]:
            el = page.query_selector(sel)
            if el:
                address = el.inner_text().strip()
                break

        # Catégorie
        category = None
        for sel in [".DkEaL", "button[jsaction*='category']"]:
            el = page.query_selector(sel)
            if el:
                category = el.inner_text().strip()
                break

        # Place ID
        place_id = ""
        m = re.search(r"place/[^/]+/([^/@?]+)", page.url)
        if m:
            place_id = m.group(1)

        rating_text = ""
        if rating:
            rating_text = f"{rating}/5 ({review_count} avis)" if review_count else f"{rating}/5"

        found = rating is not None or review_count > 0 or bool(place_name)
        log.info(
            f"  [Maps] {'✓' if found else '✗'} "
            f"{place_name or name}: {rating_text or '—'} | "
            f"☎ {phone or '—'} | 🌐 {website or '—'}"
        )

        return {
            "found": found, "rating": rating, "review_count": review_count,
            "rating_text": rating_text, "place_id": place_id,
            "place_name": place_name, "place_url": page.url,
            "phone": phone, "website": website,
            "address": address, "category": category,
        }

    except PWTimeout:
        log.warning(f"  [Maps] Timeout: {name}")
        return result
    except Exception as e:
        # Ignorer les erreurs de page fermée (elles seront gérées dans main)
        if "closed" in str(e).lower():
            log.warning(f"  [Maps] Page fermée (sera recréée)")
        else:
            log.error(f"  [Maps] Erreur: {e}")
        return result


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 3 — Pages Jaunes (Playwright)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_pages_jaunes(page: Page, name: str, city: str) -> dict:
    result = {"phone": None, "website": None}
    try:
        # Handle None values - default to empty strings
        name = name or ""
        city = city or ""

        # If both are empty, can't search
        if not name.strip() and not city.strip():
            log.warning(f"  [PJ] Pas de nom ni de ville")
            return result

        q   = urllib.parse.quote_plus(name)
        loc = urllib.parse.quote_plus(city)
        page.goto(
            f"https://www.pagesjaunes.fr/annuaire/chercherlp?quoiqui={q}&ou={loc}",
            timeout=PAGE_TIMEOUT, wait_until="domcontentloaded",
        )
        try:
            page.click("#didomi-notice-agree-button", timeout=2500)
            page.wait_for_timeout(600)
        except Exception:
            pass
        try:
            page.wait_for_selector(".bi-content, .no-result", timeout=6000)
        except Exception:
            pass

        for sel in ["[data-pj-phone]", "[class*='phone-number']", "span[class*='phone']"]:
            el = page.query_selector(sel)
            if el:
                raw = (
                    el.get_attribute("data-pj-phone")
                    or el.get_attribute("data-phone")
                    or el.inner_text()
                )
                result["phone"] = clean_phone(raw)
                if result["phone"]:
                    break

        if not result["phone"]:
            try:
                contacts = extract_best_contacts(page.inner_text(".bi-content"))
                result["phone"] = contacts["phone"]
            except Exception:
                pass

        for sel in ["a[data-pjtrack*='site']", "a[class*='site-internet']"]:
            el = page.query_selector(sel)
            if el:
                href = el.get_attribute("href") or ""
                if href and "pagesjaunes" not in href:
                    result["website"] = href.rstrip("/")
                    break

        if result["phone"] or result["website"]:
            log.info(f"  [PJ] ✓ ☎ {result['phone'] or '—'} | 🌐 {result['website'] or '—'}")
        else:
            log.info(f"  [PJ] ✗ Rien")

    except Exception as e:
        # Ignorer les erreurs de page fermée (elles seront gérées dans main)
        if "closed" in str(e).lower():
            log.warning(f"  [PJ] Page fermée (sera recréée)")
        else:
            log.warning(f"  [PJ] Erreur: {e}")
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCE 4 — Scraping profond du site web
# ══════════════════════════════════════════════════════════════════════════════

def find_contact_links(page: Page, base_url: str) -> list[str]:
    """
    Trouve dans la page tous les liens qui mènent à une page de contact.
    Cherche dans : href, texte du lien, attribut aria-label, title.
    """
    links = []
    seen  = set()

    try:
        anchors = page.query_selector_all("a[href]")
        for a in anchors:
            href  = (a.get_attribute("href") or "").strip()
            text  = (a.inner_text() or "").strip()
            label = (a.get_attribute("aria-label") or "").strip()
            title = (a.get_attribute("title") or "").strip()

            combined = f"{href} {text} {label} {title}"

            # Ignorer les liens parasites
            if SKIP_KEYWORDS.search(combined):
                continue

            # Garder uniquement si ça ressemble à une page contact
            if not CONTACT_KEYWORDS.search(combined):
                continue

            # Résoudre les URLs relatives
            if href.startswith("http"):
                full = href
            elif href.startswith("/"):
                parsed = urllib.parse.urlparse(base_url)
                full = f"{parsed.scheme}://{parsed.netloc}{href}"
            elif href.startswith("#") or href.startswith("mailto") or href.startswith("tel"):
                # mailto: peut contenir un email directement !
                if href.startswith("mailto:"):
                    email = href[7:].split("?")[0].strip()
                    if is_valid_email(email):
                        links.insert(0, f"__mailto__{email}")
                continue
            else:
                full = base_url.rstrip("/") + "/" + href

            # Éviter les doublons et les liens externes (hors domaine)
            site_domain = get_site_domain(base_url)
            link_domain = get_site_domain(full)
            if link_domain != site_domain:
                continue
            if full not in seen:
                seen.add(full)
                links.append(full)

    except Exception as e:
        log.debug(f"  [Web] Erreur find_contact_links: {e}")

    return links[:WEBSITE_MAX_LINKS]


def scrape_website_deep(page: Page, website_url: str) -> dict:
    """
    Scraping profond d'un site web :
    1. Charge la page d'accueil
    2. Cherche les liens "contact" dans le menu/footer
    3. Visite chaque lien trouvé
    4. Extrait email + téléphone avec validation stricte
    5. Fallback : URLs fixes /contact, /nous-contacter, etc.
    """
    result = {"email": None, "phone": None}
    if not website_url:
        return result

    site_domain = get_site_domain(website_url)
    visited     = set()
    contact_links = []

    # ── Étape 1 : page d'accueil ──────────────────────────────────────────────
    try:
        page.goto(website_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        visited.add(website_url)

        # Chercher mailto: directement dans la page d'accueil
        body_text = page.inner_text("body")
        contacts  = extract_best_contacts(body_text, site_domain)
        if contacts["email"]:
            result["email"] = contacts["email"]
        if contacts["phone"]:
            result["phone"] = contacts["phone"]

        # Chercher aussi les balises mailto dans le HTML
        try:
            html = page.content()
            mailtos = re.findall(r'mailto:([^\s"\'?<>]+)', html)
            for m in mailtos:
                if is_valid_email(m, site_domain) and not result["email"]:
                    result["email"] = m
        except Exception:
            pass

        if result["email"] and result["phone"]:
            log.info(f"  [Web] ✓ Trouvé dès la home: 📧 {result['email']} ☎ {result['phone']}")
            return result

        # ── Étape 2 : trouver les liens "contact" ─────────────────────────────
        contact_links = find_contact_links(page, website_url)
        log.debug(f"  [Web] {len(contact_links)} lien(s) contact trouvé(s): {contact_links}")

    except Exception as e:
        log.debug(f"  [Web] Erreur page d'accueil ({website_url}): {e}")

    # ── Étape 3 : visiter les liens contact ────────────────────────────────────
    for link in contact_links:
        if result["email"] and result["phone"]:
            break

        # Cas spécial : lien mailto extrait directement
        if link.startswith("__mailto__"):
            email = link[10:]
            if is_valid_email(email, site_domain) and not result["email"]:
                result["email"] = email
                log.info(f"  [Web] ✓ mailto trouvé: 📧 {email}")
            continue

        if link in visited:
            continue
        visited.add(link)

        try:
            page.goto(link, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            page.wait_for_timeout(500)

            body_text = page.inner_text("body")
            contacts  = extract_best_contacts(body_text, site_domain)

            if contacts["email"] and not result["email"]:
                result["email"] = contacts["email"]
            if contacts["phone"] and not result["phone"]:
                result["phone"] = contacts["phone"]

            # Chercher mailto dans le HTML aussi
            try:
                html = page.content()
                mailtos = re.findall(r'mailto:([^\s"\'?<>]+)', html)
                for m in mailtos:
                    if is_valid_email(m, site_domain) and not result["email"]:
                        result["email"] = m
            except Exception:
                pass

        except Exception as e:
            log.debug(f"  [Web] Erreur visite {link}: {e}")
            continue

    # ── Étape 4 : fallback URLs fixes ─────────────────────────────────────────
    if not result["email"] or not result["phone"]:
        base = website_url.rstrip("/")
        fallback_paths = [
            "/contact", "/nous-contacter", "/contact.html",
            "/contactez-nous", "/contact-us", "/joindre",
            "/a-propos", "/about", "/qui-sommes-nous",
            "/informations", "/coordonnees",
        ]
        for path in fallback_paths:
            url = base + path
            if url in visited:
                continue
            visited.add(url)
            try:
                page.goto(url, timeout=8000, wait_until="domcontentloaded")
                body_text = page.inner_text("body")
                contacts  = extract_best_contacts(body_text, site_domain)
                if contacts["email"] and not result["email"]:
                    result["email"] = contacts["email"]
                if contacts["phone"] and not result["phone"]:
                    result["phone"] = contacts["phone"]
                if result["email"] and result["phone"]:
                    break
            except Exception:
                continue

    if result["email"] or result["phone"]:
        log.info(
            f"  [Web] ✓ 📧 {result['email'] or '—'} | ☎ {result['phone'] or '—'} "
            f"({len(visited)} page(s) visitée(s))"
        )
    else:
        log.info(f"  [Web] ✗ Aucun contact ({len(visited)} page(s) visitée(s))")

    return result


# ══════════════════════════════════════════════════════════════════════════════
#  BASE DE DONNÉES — PostgreSQL (Neon) Direct
# ══════════════════════════════════════════════════════════════════════════════

def get_conn():
    """Crée une connexion PostgreSQL."""
    return psycopg2.connect(DB_URL, connect_timeout=10)


def ensure_db_connected(conn):
    """Vérifie que la connexion est active, sinon la recrée."""
    try:
        if conn and not conn.closed:
            # Test la connexion avec un simple SELECT
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
    except Exception as e:
        log.warning(f"  Reconnexion DB nécessaire: {str(e)[:50]}")

    # Créer une nouvelle connexion
    try:
        return get_conn()
    except Exception as e:
        log.error(f"  ❌ Impossible de se reconnecter: {e}")
        return None


def ensure_table(conn):
    """Crée la table google_reviews si elle n'existe pas."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS google_reviews (
                siret         VARCHAR(14)  PRIMARY KEY,
                found         BOOLEAN      NOT NULL DEFAULT FALSE,
                rating        REAL,
                review_count  INTEGER      NOT NULL DEFAULT 0,
                rating_text   VARCHAR(50),
                place_id      VARCHAR(100),
                place_name    VARCHAR(255),
                place_url     TEXT,
                phone         VARCHAR(20),
                email         VARCHAR(255),
                website       TEXT,
                address       TEXT,
                category      VARCHAR(255),
                phone_source  VARCHAR(50),
                email_source  VARCHAR(50),
                scraped_at    TIMESTAMP    NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS gr_rating_idx     ON google_reviews(rating);
            CREATE INDEX IF NOT EXISTS gr_scraped_at_idx ON google_reviews(scraped_at);
        """)
        conn.commit()
    log.info("  ✓ Table google_reviews prête")


def fetch_pending_db(conn, batch_size: int, offset: int) -> list[dict]:
    """
    Récupère les entreprises non enrichies directement depuis PostgreSQL.
    Détecte la table principale automatiquement et cherche les colonnes siret/nom/ville.
    """
    # Déterminer le nom de la table principale
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [r[0] for r in cur.fetchall()]
    log.info(f"  Tables détectées: {tables[:3]}...")

    # Chercher la table qui contient les entreprises BTP
    candidates = [t for t in tables if any(
        kw in t.lower() for kw in ["company", "compan", "entreprise", "etablissement", "btp", "sirene"]
    )]
    if not candidates:
        candidates = [t for t in tables if t not in ("google_reviews",)]

    if not candidates:
        raise ValueError(f"Aucune table entreprise trouvée. Tables: {tables}")

    main_table = candidates[0]
    log.info(f"  Table principale: {main_table}")

    # Déterminer les colonnes disponibles
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{main_table}'
            ORDER BY ordinal_position;
        """)
        cols = [r[0] for r in cur.fetchall()]
    log.debug(f"  Colonnes: {cols[:5]}...")

    # Mapper les colonnes
    col_siret = next((c for c in cols if "siret" in c.lower()), None)

    # Chercher la colonne nom/denomination (case-insensitive)
    # Priorité: raison_sociale (standard FR) → nom_commercial → autres noms
    col_name  = next((c for c in cols if c.lower() in (
        "raison_sociale",  # French standard - has data
        "nom_entreprise", "denomination", "denominationusuelleetablissement",
        "denominationunite", "name", "nom", "company_name", "nom_commercial"
    )), None)
    if not col_name:
        # Si pas trouvé exactement, chercher les colonnes contenant "raison", "name", "nom", "denomination"
        col_name = next((c for c in cols if any(x in c.lower() for x in ["raison", "name", "nom", "denomination"])), None)

    # Chercher la colonne ville/commune (case-insensitive)
    col_city  = next((c for c in cols if c.lower() in (
        "libellecommuneetablissement", "commune", "city", "ville", "libelle_commune", "communne"
    )), None)
    if not col_city:
        # Si pas trouvé exactement, chercher les colonnes contenant "commune", "city", "ville"
        col_city = next((c for c in cols if any(x in c.lower() for x in ["commune", "city", "ville"])), None)

    if not col_siret:
        raise ValueError(f"Colonne SIRET introuvable dans {main_table}. Colonnes: {cols}")

    log.debug(f"  Colonnes détectées: SIRET={col_siret}, NOM={col_name}, VILLE={col_city}")
    if not col_name:
        log.warning(f"  ⚠️  Colonne NOM non trouvée - utilisera NULL")
    if not col_city:
        log.warning(f"  ⚠️  Colonne VILLE non trouvée - utilisera NULL")

    select_name = f'"{col_name}"' if col_name else "NULL"
    select_city = f'"{col_city}"' if col_city else "NULL"

    cutoff = "NOW() - INTERVAL '30 days'"

    query = f"""
        SELECT
            e."{col_siret}"      AS siret,
            {select_name}        AS name,
            {select_city}        AS city
        FROM "{main_table}" e
        LEFT JOIN google_reviews gr ON gr.siret = e."{col_siret}"::varchar
        WHERE gr.siret IS NULL
           OR gr.scraped_at < {cutoff}
        ORDER BY e."{col_siret}"
        LIMIT %s OFFSET %s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (batch_size, offset))
        rows = cur.fetchall()

    return [dict(r) for r in rows]


def count_pending_db(conn) -> int:
    """Compte le total d'entreprises restant à enrichir."""
    with conn.cursor() as cur:
        # Récupérer la table principale
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [r[0] for r in cur.fetchall()]
        candidates = [t for t in tables if t not in ("google_reviews",)]
        if not candidates:
            return 0
        main_table = candidates[0]

        # Trouver col siret
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{main_table}'
              AND column_name ILIKE '%siret%'
            LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            return 0
        col_siret = row[0]

        cur.execute(f"""
            SELECT COUNT(*) FROM "{main_table}" e
            LEFT JOIN google_reviews gr ON gr.siret = e."{col_siret}"::varchar
            WHERE gr.siret IS NULL
               OR gr.scraped_at < NOW() - INTERVAL '30 days';
        """)
        return cur.fetchone()[0]


def upsert_result(conn, result: EnrichResult, dry_run: bool = False) -> bool:
    """Insère ou met à jour une ligne dans google_reviews."""
    if dry_run:
        log.info(f"  [DRY-RUN] UPSERT siret={result.siret} rating={result.rating_text} phone={result.phone} email={result.email}")
        return True
    try:
        # Vérifier que la connexion est toujours valide
        try:
            _ = conn.closed
        except Exception:
            # Connexion fermée, ne rien faire
            log.error(f"  ❌ Connexion DB fermée ({result.siret})")
            return False

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO google_reviews (
                    siret, found, rating, review_count, rating_text,
                    place_id, place_name, place_url,
                    phone, email, website, address, category,
                    phone_source, email_source, scraped_at
                ) VALUES (
                    %(siret)s, %(found)s, %(rating)s, %(review_count)s, %(rating_text)s,
                    %(place_id)s, %(place_name)s, %(place_url)s,
                    %(phone)s, %(email)s, %(website)s, %(address)s, %(category)s,
                    %(phone_source)s, %(email_source)s, %(scraped_at)s
                )
                ON CONFLICT (siret) DO UPDATE SET
                    found        = EXCLUDED.found,
                    rating       = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count,
                    rating_text  = EXCLUDED.rating_text,
                    place_id     = EXCLUDED.place_id,
                    place_name   = EXCLUDED.place_name,
                    place_url    = EXCLUDED.place_url,
                    phone        = EXCLUDED.phone,
                    email        = EXCLUDED.email,
                    website      = EXCLUDED.website,
                    address      = EXCLUDED.address,
                    category     = EXCLUDED.category,
                    phone_source = EXCLUDED.phone_source,
                    email_source = EXCLUDED.email_source,
                    scraped_at   = EXCLUDED.scraped_at;
            """, {
                "siret":        result.siret,
                "found":        result.found,
                "rating":       result.rating,
                "review_count": result.review_count,
                "rating_text":  result.rating_text,
                "place_id":     result.place_id,
                "place_name":   result.place_name,
                "place_url":    result.place_url,
                "phone":        result.phone,
                "email":        result.email,
                "website":      result.website,
                "address":      result.address,
                "category":     result.category,
                "phone_source": result.phone_source,
                "email_source": result.email_source,
                "scraped_at":   result.scraped_at,
            })
        conn.commit()
        return True
    except Exception as e:
        try:
            if conn and not conn.closed:
                conn.rollback()
        except Exception:
            pass  # Connexion déjà fermée, ignorer
        log.error(f"  ❌ UPSERT échoué ({result.siret}): {e}")
        return False


def print_stats_db(conn):
    """Affiche les stats directement depuis la DB."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                          AS total,
                    COUNT(*) FILTER (WHERE found = TRUE)             AS with_rating,
                    COUNT(*) FILTER (WHERE email IS NOT NULL)        AS with_email,
                    COUNT(*) FILTER (WHERE phone IS NOT NULL)        AS with_phone,
                    COUNT(*) FILTER (WHERE website IS NOT NULL)      AS with_website,
                    COUNT(*) FILTER (WHERE scraped_at > NOW() - INTERVAL '24 hours') AS last_24h,
                    AVG(rating) FILTER (WHERE rating IS NOT NULL)    AS avg_rating
                FROM google_reviews;
            """)
            s = cur.fetchone()
            avg_rating = f"{s[6]:.2f}" if s[6] else "—"
            print(f"\n  📊 DB Stats:")
            print(f"     Total enrichis  : {s[0]:,}")
            print(f"     Avec avis Google: {s[1]:,}")
            print(f"     Avec email      : {s[2]:,}")
            print(f"     Avec téléphone  : {s[3]:,}")
            print(f"     Avec site web   : {s[4]:,}")
            print(f"     Dernières 24h   : {s[5]:,}")
            print(f"     Note moyenne    : {avg_rating}")
    except Exception as e:
        log.warning(f"  Stats DB: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATEUR
# ══════════════════════════════════════════════════════════════════════════════

def enrich_one(page: Page, company: dict, gouv_data: dict) -> EnrichResult:
    """
    Enrichit une entreprise en cascade.
    gouv_data : résultat pré-calculé de l'API gouv pour ce SIRET.
    """
    siret = company.get("siret", "")
    name  = company.get("name", company.get("nom_entreprise", "")) or ""
    city  = company.get("city", company.get("libelleCommuneEtablissement", "")) or ""

    phone = email = website = None
    phone_source = email_source = ""

    # Pré-remplissage depuis l'API gouv
    if gouv_data.get("website"):
        website = gouv_data["website"]
        log.info(f"  [API] 🌐 {website}")
    if gouv_data.get("phone"):
        phone = gouv_data["phone"]
        phone_source = "api-gouv"

    # ── Google Maps ───────────────────────────────────────────────────────────
    maps = scrape_google_maps(page, name, city)

    if maps.get("phone") and not phone:
        phone = maps["phone"]
        phone_source = "google-maps"
    if maps.get("website") and not website:
        website = maps["website"]

    # ── Pages Jaunes (si téléphone ou site manquant) ──────────────────────────
    if not phone or not website:
        log.info(f"  → Pages Jaunes")
        pj = scrape_pages_jaunes(page, name, city)
        if pj.get("phone") and not phone:
            phone = pj["phone"]
            phone_source = "pages-jaunes"
        if pj.get("website") and not website:
            website = pj["website"]

    # ── Scraping profond du site web ──────────────────────────────────────────
    if website:
        log.info(f"  → Scraping profond: {website}")
        web = scrape_website_deep(page, website)
        if web.get("email"):
            email = web["email"]
            email_source = "website"
        if web.get("phone") and not phone:
            phone = web["phone"]
            phone_source = "website"

    log.info(
        f"  📊 FINAL | note={maps.get('rating_text') or '—'} | "
        f"☎ {phone or '—'} [{phone_source}] | "
        f"📧 {email or '—'} [{email_source}] | "
        f"🌐 {website or '—'}"
    )

    return EnrichResult(
        siret=siret, found=maps["found"],
        rating=maps["rating"], review_count=maps["review_count"],
        rating_text=maps["rating_text"], place_id=maps["place_id"],
        place_name=maps["place_name"], place_url=maps["place_url"],
        phone=phone, website=website, email=email,
        address=maps["address"], category=maps["category"],
        phone_source=phone_source, email_source=email_source,
        scraped_at=datetime.now(timezone.utc).isoformat(),
    )


# ══════════════════════════════════════════════════════════════════════════════
#  COMMUNICATION API KTIPIO
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="KTIPIO Enrichissement v4 — Direct DB")
    p.add_argument("--limit",           type=int,  default=None,  help="Max entreprises total")
    p.add_argument("--offset",          type=int,  default=0,     help="Offset de départ (reprise)")
    p.add_argument("--batch-size",      type=int,  default=API_BATCH_SIZE)
    p.add_argument("--api-concurrent",  type=int,  default=API_CONCURRENT,
                   help=f"Requêtes parallèles API gouv (défaut: {API_CONCURRENT}, max: 10)")
    p.add_argument("--dry-run",         action="store_true")
    p.add_argument("--headed",          action="store_true",       help="Afficher Chromium (défaut: headless)")
    p.add_argument("--skip-playwright", action="store_true",       help="API gouv seulement")
    p.add_argument("--delay",           type=int,  default=PLAYWRIGHT_DELAY,
                   help=f"Secondes entre entreprises Playwright (défaut: {PLAYWRIGHT_DELAY})")
    return p.parse_args()


def main():
    args = parse_args()

    print("\n" + "═" * 65)
    print("  KTIPIO — Enrichissement Contacts BTP v4 (Direct DB)")
    print(f"  DB       : Neon PostgreSQL")
    print(f"  Mode     : {'DRY-RUN' if args.dry_run else 'Production'}")
    print(f"  Sources  : API gouv → Maps → Pages Jaunes → Site web (profond)")
    print(f"  Délai    : {args.delay}s entre entreprises")
    print(f"  Offset   : {args.offset}")
    print("═" * 65 + "\n")

    # Connexion DB
    log.info("Connexion à Neon PostgreSQL…")
    try:
        conn = get_conn()
        log.info("  ✓ Connecté")
    except Exception as e:
        log.error(f"  ❌ Connexion impossible: {e}")
        return

    ensure_table(conn)

    # Compter le total restant
    total_pending = count_pending_db(conn)
    log.info(f"  📋 {total_pending:,} entreprises à enrichir")

    total_processed = 0
    total_success   = 0
    total_found     = 0
    current_offset  = args.offset

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=not args.headed,
            args=["--lang=fr-FR", "--disable-blink-features=AutomationControlled"],
        ) if not args.skip_playwright else None

        ctx = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        ) if browser else None
        page = ctx.new_page() if ctx else None

        try:
            while True:
                if args.limit and total_processed >= args.limit:
                    break

                batch_size = args.batch_size
                if args.limit:
                    batch_size = min(batch_size, args.limit - total_processed)

                # Lire depuis la DB
                companies = fetch_pending_db(conn, batch_size, current_offset)
                if not companies:
                    log.info("✅ Plus d'entreprises à traiter.")
                    break

                log.info(f"\n{'─'*55}")
                log.info(f"  OFFSET {current_offset} — {len(companies)} entreprises")
                log.info(f"  Progression: {total_processed:,}/{total_pending:,}")
                log.info(f"{'─'*55}")

                # Étape 1 : API gouv en masse
                log.info(f"\n  → Étape 1/4 : API gouv en masse ({len(companies)} req, {args.api_concurrent} concurrent)…")
                gouv_results = asyncio.run(bulk_api_gouv(companies, concurrent=args.api_concurrent))

                # Étape 2 : Playwright entreprise par entreprise
                if not args.skip_playwright and page:
                    log.info(f"\n  → Étape 2/4 : Playwright (Maps + PJ + Site web)…\n")
                    for i, company in enumerate(companies):
                        if args.limit and total_processed >= args.limit:
                            break

                        # Vérifier que la page est valide, sinon la recréer
                        page = ensure_page_valid(page, ctx)

                        siret  = company.get("siret", "")
                        name   = company.get("name", company.get("nom_entreprise", ""))
                        city   = company.get("city", company.get("libelleCommuneEtablissement", ""))
                        gouv   = gouv_results.get(siret, {})

                        log.info(f"\n  [{total_processed+1}/{total_pending:,}] {name} — {city}")

                        result = enrich_one(page, company, gouv)

                        # Assurer la connexion DB avant d'écrire
                        conn = ensure_db_connected(conn)
                        if not conn:
                            log.error(f"  ❌ Impossible de continuer sans DB")
                            break

                        if upsert_result(conn, result, dry_run=args.dry_run):
                            total_success += 1
                        if result.found:
                            total_found += 1
                        total_processed += 1

                        if i < len(companies) - 1:
                            log.info(f"  ⏳ {args.delay}s…")
                            time.sleep(args.delay)
                else:
                    # Mode API gouv seulement
                    for company in companies:
                        if args.limit and total_processed >= args.limit:
                            break
                        siret = company.get("siret", "")
                        gouv  = gouv_results.get(siret, {})
                        r     = empty_result(siret)
                        r.website      = gouv.get("website")
                        r.phone        = gouv.get("phone")
                        r.phone_source = "api-gouv" if gouv.get("phone") else ""
                        r.scraped_at   = datetime.now(timezone.utc).isoformat()
                        if upsert_result(conn, r, dry_run=args.dry_run):
                            total_success += 1
                        total_processed += 1

                # Stats après chaque batch
                print_stats_db(conn)

                if len(companies) < batch_size:
                    log.info("✅ Dernière page.")
                    break

                current_offset += len(companies)

        except KeyboardInterrupt:
            log.info("\n⛔ Arrêt (Ctrl+C)")
        finally:
            if browser:
                browser.close()
            conn.close()

    print("\n" + "═" * 65)
    print(f"  ✅ Terminé")
    print(f"     Traités  : {total_processed:,}")
    print(f"     En DB    : {total_success:,}")
    print(f"     Avec avis: {total_found:,}")
    print(f"     Reprendre: --offset {current_offset}")
    print("═" * 65 + "\n")


if __name__ == "__main__":
    main()