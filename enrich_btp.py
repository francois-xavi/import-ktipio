import pandas as pd
import time
import re
import json
import requests
import urllib.parse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
INPUT_FILE  = "btp_complet.parquet"
OUTPUT_FILE = "btp_enrichi.csv"
OUTPUT_JSON = "btp_enrichi.json"
BATCH_SIZE  = 50
DELAY       = 4   # seconds between companies (be polite to servers)

COL_NAME          = "denominationUsuelleEtablissement"
COL_NAME_FALLBACK = "enseigne1Etablissement"
COL_CITY          = "libelleCommuneEtablissement"
COL_SIRET         = "siret"   # set to None if column doesn't exist

# French phone regex
FR_PHONE  = re.compile(r'(?:(?:\+|00)33[\s.\-]?|0)[1-9](?:[\s.\-]?\d{2}){4}')
EMAIL_RE  = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
BAD_EMAIL = {"google", "gstatic", "schema", "example", "sentry",
             "noreply", "no-reply", "wix", "wordpress"}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def save_json(df: pd.DataFrame, path: str):
    """Write JSON row by row — avoids MemoryError and timestamp overflow on large DataFrames."""
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, (_, row) in enumerate(df.iterrows()):
            record = {}
            for col, val in row.items():
                if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                    record[col] = None
                elif hasattr(val, "isoformat"):      # datetime → string
                    record[col] = val.isoformat()
                elif isinstance(val, float) and (val != val):  # NaN guard
                    record[col] = None
                else:
                    try:
                        record[col] = val.item()     # numpy scalar → python native
                    except AttributeError:
                        record[col] = val
            comma = "," if i < len(df) - 1 else ""
            f.write(f"  {json.dumps(record, ensure_ascii=False)}{comma}\n")
        f.write("]\n")


def safe_str(val) -> str:
    """Convert any value to a clean string, returning '' for NaN/None/float."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def clean_phone(raw: str) -> str:
    """Normalise phone to digits only (keeps leading +)."""
    return re.sub(r"[^\d+]", "", raw.strip())


def extract_contacts(text: str) -> dict:
    """Extract first phone and email from a block of text."""
    phones = FR_PHONE.findall(text)
    emails = [e for e in EMAIL_RE.findall(text)
              if not any(b in e.lower() for b in BAD_EMAIL)]
    return {
        "phone": clean_phone(phones[0]) if phones else None,
        "email": emails[0] if emails else None,
    }


# ─────────────────────────────────────────────
# SOURCE 1 — recherche-entreprises.api.gouv.fr
# Free official API — gives website URL
# Does NOT contain phone numbers (SIRENE limitation)
# ─────────────────────────────────────────────
def search_api_gouv(company_name: str, city: str, siret: str = None) -> dict:
    result = {"website": None, "email": None, "phone": None, "source": None}
    try:
        q = f"{company_name} {city}".strip()
        r = requests.get(
            "https://recherche-entreprises.api.gouv.fr/search",
            params={"q": q, "per_page": 3},
            timeout=8
        )
        if r.status_code == 200 and "json" in r.headers.get("Content-Type", ""):
            for hit in r.json().get("results", []):
                site = hit.get("site_internet") or hit.get("url")
                if site:
                    result["website"] = site
                    result["source"] = "api.gouv.fr"
                    break
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────
# SOURCE 2 — Google Maps (Playwright)
# This is EXACTLY the data you see when you
# manually Google a company name + city.
# The knowledge panel phone comes from here.
# ─────────────────────────────────────────────
def is_french_phone(phone: str) -> bool:
    """Check the phone number is French (+33 or 0X)."""
    if not phone:
        return False
    p = re.sub(r"[\s.\-]", "", phone)
    return p.startswith("+33") or p.startswith("0033") or (p.startswith("0") and len(p) >= 9)


def search_google_maps(page, company_name: str, city: str) -> dict:
    result = {"website": None, "email": None, "phone": None, "source": None}
    try:
        # Always include city + France to avoid matching foreign companies
        q = urllib.parse.quote_plus(f"{company_name} {city} France")
        page.goto(
            f"https://www.google.com/maps/search/{q}?hl=fr",
            timeout=15000,
            wait_until="domcontentloaded"
        )

        # Accept Google consent popup if present
        try:
            page.click(
                "button[aria-label*='Tout accepter'], button[aria-label*='Accept all']",
                timeout=3000
            )
            page.wait_for_timeout(1500)
        except Exception:
            pass

        # Click the first result in the list to open its panel
        try:
            page.click(".hfpxzc", timeout=6000)
            page.wait_for_timeout(2000)
        except Exception:
            pass

        # Read the full page text (panel is in the DOM)
        text = page.inner_text("body")

        # Validate result is in France — check address contains France or a French dept/city
        # If the panel shows a non-French phone, discard it
        contacts = extract_contacts(text)
        phone = contacts["phone"]
        if phone and not is_french_phone(phone):
            phone = None   # foreign number — discard

        result["phone"] = phone
        result["email"] = contacts["email"]

        # Grab the official website link from the panel
        for selector in [
            "a[data-item-id='authority']",
            "a[aria-label*='Site Web']",
            "a[aria-label*='Website']",
            "a[href*='http'][data-tooltip*='web']",
        ]:
            el = page.query_selector(selector)
            if el:
                result["website"] = el.get_attribute("href")
                break

        if result["phone"] or result["website"]:
            result["source"] = "google-maps"

    except PWTimeout:
        pass
    except Exception as e:
        print(f"    [google-maps] {e}")
    return result


# ─────────────────────────────────────────────
# SOURCE 3 — Pages Jaunes (Playwright)
# Real browser avoids bot detection.
# Good secondary source for phone numbers.
# ─────────────────────────────────────────────
def search_pages_jaunes(page, company_name: str, city: str) -> dict:
    result = {"website": None, "email": None, "phone": None, "source": None}
    try:
        q   = urllib.parse.quote_plus(company_name)
        loc = urllib.parse.quote_plus(city)
        url = f"https://www.pagesjaunes.fr/annuaire/chercherlp?quoiqui={q}&ou={loc}"
        page.goto(url, timeout=15000, wait_until="domcontentloaded")

        # Accept cookies if banner appears
        try:
            page.click("#didomi-notice-agree-button", timeout=3000)
            page.wait_for_timeout(1000)
        except Exception:
            pass

        # Wait for results or no-result indicator
        try:
            page.wait_for_selector(".bi-content, .no-result", timeout=8000)
        except Exception:
            pass

        # ── Phone — PJ hides it behind JS, check data attributes first ──
        for selector in [
            "[data-pj-phone]",
            "[class*='phone-number']",
            "[class*='tel-']",
            "span[class*='phone']",
        ]:
            el = page.query_selector(selector)
            if el:
                raw = (el.get_attribute("data-pj-phone")
                       or el.get_attribute("data-phone")
                       or el.inner_text())
                if raw:
                    result["phone"] = clean_phone(raw)
                    break

        # ── Fallback: regex visible text ──
        if not result["phone"]:
            try:
                text = page.inner_text(".bi-content")
                contacts = extract_contacts(text)
                result["phone"] = contacts["phone"]
                result["email"] = contacts["email"]
            except Exception:
                pass

        # ── Website ──
        for selector in [
            "a[data-pjtrack*='site']",
            "a[class*='site-internet']",
            "a[href*='http'][class*='website']",
        ]:
            el = page.query_selector(selector)
            if el:
                result["website"] = el.get_attribute("href")
                break

        if result["phone"] or result["website"]:
            result["source"] = "pages-jaunes"

    except PWTimeout:
        pass
    except Exception as e:
        print(f"    [pages-jaunes] {e}")
    return result


# ─────────────────────────────────────────────
# SOURCE 4 — Company website (Playwright)
# Scrapes /contact page for email + phone
# ─────────────────────────────────────────────
def scrape_website(page, url: str) -> dict:
    result = {"email": None, "phone": None}
    if not url:
        return result
    for path in ["/contact", "/nous-contacter", "/contact.html", ""]:
        try:
            page.goto(url.rstrip("/") + path, timeout=10000, wait_until="domcontentloaded")
            text = page.inner_text("body")
            contacts = extract_contacts(text)
            if contacts["email"]:
                result["email"] = contacts["email"]
            if contacts["phone"]:
                result["phone"] = contacts["phone"]
            if result["email"] or result["phone"]:
                break
        except Exception:
            continue
    return result


# ─────────────────────────────────────────────
# MERGE — combine all sources with priority
# New order:
#   1. api.gouv.fr  → get official website URL
#   2. Scrape website → best source (phone + email on contact page)
#   3. Google Maps  → phone fallback (with France filter)
#   4. Pages Jaunes → last resort phone fallback
# ─────────────────────────────────────────────
def enrich_company(page, company_name: str, city: str, siret: str = None) -> dict:
    merged = {"website": None, "email": None, "phone": None, "source": None}

    def absorb(src: dict, override_phone=False, override_email=False):
        for field in ["website", "phone", "email"]:
            if field == "phone" and not override_phone and merged["phone"]:
                continue
            if field == "email" and not override_email and merged["email"]:
                continue
            if not merged[field] and src.get(field):
                merged[field] = src[field]
                if not merged["source"]:
                    merged["source"] = src.get("source", "?")

    # 1. Official API → get website URL
    absorb(search_api_gouv(company_name, city, siret))

    # 2. Scrape the company website for phone + email (most reliable)
    if merged["website"]:
        site_data = scrape_website(page, merged["website"])
        if site_data["phone"]:
            merged["phone"] = site_data["phone"]
            merged["source"] = merged["source"] or "website"
        if site_data["email"]:
            merged["email"] = site_data["email"]

    # 3. Google Maps — only if still missing phone (adds France to query)
    if not merged["phone"]:
        maps_data = search_google_maps(page, company_name, city)
        absorb(maps_data)

    # 4. Pages Jaunes — last resort
    if not merged["phone"]:
        absorb(search_pages_jaunes(page, company_name, city))

    return merged


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    # ── Load data ──
    if INPUT_FILE.endswith(".parquet"):
        df = pd.read_parquet(INPUT_FILE)
    elif INPUT_FILE.endswith(".json"):
        df = pd.read_json(INPUT_FILE)
    elif INPUT_FILE.endswith(".csv"):
        df = pd.read_csv(INPUT_FILE)
    else:
        raise ValueError(f"Unsupported format: {INPUT_FILE}")

    for col in ["website", "email", "phone", "source"]:
        if col not in df.columns:
            df[col] = None

    total = len(df)
    print(f"🚀 Enrichment starting — {total} companies")

    with sync_playwright() as p:
        # headless=False so you can watch + debug; set True for unattended runs
        browser = p.chromium.launch(
            headless=True,   # no UI window
            args=["--lang=fr-FR", "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = context.new_page()

        for i in range(0, total, BATCH_SIZE):
            batch     = df.iloc[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            n_batches = (total - 1) // BATCH_SIZE + 1
            print(f"\n📦 Batch {batch_num} / {n_batches}")

            for index, row in batch.iterrows():
                # Skip rows already fully enriched
                if pd.notnull(df.at[index, "phone"]) and pd.notnull(df.at[index, "email"]):
                    continue

                name  = safe_str(row.get(COL_NAME)) or safe_str(row.get(COL_NAME_FALLBACK, ""))
                city  = safe_str(row.get(COL_CITY, ""))
                siret = safe_str(row.get(COL_SIRET, "")) if COL_SIRET else ""
                siret = siret or None

                if not name:
                    continue

                res = enrich_company(page, name, city, siret)

                df.at[index, "website"] = res["website"]
                df.at[index, "email"]   = res["email"]
                df.at[index, "phone"]   = res["phone"]
                df.at[index, "source"]  = res["source"]

                ph = f"📞 {res['phone']}" if res["phone"] else "❌ no phone"
                em = f"📧 {res['email']}" if res["email"] else "no email"
                print(f"   ✅ {name[:40]:<40} → {ph} | {em} [{res['source'] or 'none'}]")

                time.sleep(DELAY)

            # Save after each batch (crash-safe)
            df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
            save_json(df, OUTPUT_JSON)
            print(f"   💾 Saved — batch {batch_num} done")

        browser.close()

    # ── Final summary ──
    found_phone = df["phone"].notna().sum()
    found_email = df["email"].notna().sum()
    print(f"\n✅ Done!")
    print(f"   📞 Phone : {found_phone}/{total} ({100*found_phone//total}%)")
    print(f"   📧 Email : {found_email}/{total} ({100*found_email//total}%)")
    print(f"   📁 CSV   : {OUTPUT_FILE}")
    print(f"   📁 JSON  : {OUTPUT_JSON}")


if __name__ == "__main__":
    main()