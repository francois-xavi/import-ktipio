#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           BTP COMPANIES DATABASE BUILDER — Elysian / Tim                    ║
║  Sectors: NAF divisions 41 (construction bâtiment), 42 (génie civil),       ║
║           43 (travaux de construction spécialisés)                          ║
║  Sources: INSEE SIRENE (via data.gouv.fr) + Annuaire Entreprises API        ║
║  Output:  CSV avec tous les établissements + enrichissement optionnel        ║
╚══════════════════════════════════════════════════════════════════════════════╝

USAGE:
    python btp_db_builder.py                    # SIRENE only (fast, ~5 min)
    python btp_db_builder.py --enrich           # + Annuaire API (slow, hours)
    python btp_db_builder.py --enrich --limit 500   # test enrichment on 500 rows
    python btp_db_builder.py --dept 75,69,13    # filter by département(s)
    python btp_db_builder.py --active-only      # exclude closed establishments

REQUIREMENTS:
    pip install polars pyproj httpx requests tqdm

NOTE ON ENRICHMENT:
    website / email / phone → Annuaire des Entreprises API (public, free, ~30% coverage)
    google_reviews          → requires your own Google Places API key (see config below)
    latitude / longitude    → converted from Lambert93 (always available when present)
"""

import os
import sys
import time
import json
import logging
import zipfile
import argparse
import asyncio
from io import BytesIO
from pathlib import Path
from typing import Optional

import httpx
import polars as pl
import requests
from tqdm import tqdm

# ─── Optional imports ─────────────────────────────────────────────────────────
try:
    from pyproj import Transformer
    HAS_PYPROJ = True
except ImportError:
    HAS_PYPROJ = False
    print("⚠  pyproj not installed — Lambert→WGS84 conversion disabled (pip install pyproj)")

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

NAF_PREFIXES = ("41", "42", "43")  # BTP divisions

# Directories
SCRIPT_DIR  = Path(__file__).parent
CACHE_DIR   = SCRIPT_DIR / ".sirene_cache"
OUTPUT_DIR  = SCRIPT_DIR
OUTPUT_FILE = OUTPUT_DIR / "btp_companies.csv"

# data.gouv.fr
DATAGOUV_API  = "https://www.data.gouv.fr/api/1"
SIRENE_SLUG   = "base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret"

# Annuaire des Entreprises (public French government API — no key needed)
ANNUAIRE_SEARCH_API  = "https://recherche-entreprises.api.gouv.fr/search"
ANNUAIRE_ETAB_API    = "https://annuaire-entreprises.data.gouv.fr/api/v3/etablissement"

# ── Optional: Google Places API ───────────────────────────────────────────────
# Set your key here or via env var GOOGLE_PLACES_API_KEY to get google_reviews
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

# Rate limiting
ENRICH_CONCURRENT    = 8      # parallel async requests
ENRICH_DELAY         = 0.12   # seconds between requests (≈ 8 req/s)
GOOGLE_DELAY         = 0.2

# ── SIRENE columns we want ────────────────────────────────────────────────────
ETABLISSEMENT_COLS = [
    "siren", "nic", "siret",
    "statutDiffusionEtablissement",
    "dateCreationEtablissement",
    "trancheEffectifsEtablissement",
    "anneeEffectifsEtablissement",
    "activitePrincipaleRegistreMetiersEtablissement",
    "dateDernierTraitementEtablissement",
    "etablissementSiege",
    "nombrePeriodesEtablissement",
    "complementAdresseEtablissement",
    "numeroVoieEtablissement",
    "indiceRepetitionEtablissement",
    "dernierNumeroVoieEtablissement",
    "indiceRepetitionDernierNumeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "libelleCommuneEtrangerEtablissement",
    "distributionSpecialeEtablissement",
    "codeCommuneEtablissement",
    "codeCedexEtablissement",
    "libelleCedexEtablissement",
    "codePaysEtrangerEtablissement",
    "libellePaysEtrangerEtablissement",
    "identifiantAdresseEtablissement",
    "coordonneeLambertAbscisseEtablissement",
    "coordonneeLambertOrdonneeEtablissement",
    "complementAdresse2Etablissement",
    "numeroVoie2Etablissement",
    "indiceRepetition2Etablissement",
    "typeVoie2Etablissement",
    "libelleVoie2Etablissement",
    "codePostal2Etablissement",
    "libelleCommune2Etablissement",
    "libelleCommuneEtranger2Etablissement",
    "distributionSpeciale2Etablissement",
    "codeCommune2Etablissement",
    "codeCedex2Etablissement",
    "libelleCedex2Etablissement",
    "codePaysEtranger2Etablissement",
    "libellePaysEtranger2Etablissement",
    "dateDebut",
    "etatAdministratifEtablissement",
    "enseigne1Etablissement",
    "enseigne2Etablissement",
    "enseigne3Etablissement",
    "denominationUsuelleEtablissement",
    "activitePrincipaleEtablissement",
    "nomenclatureActivitePrincipaleEtablissement",
    "caractereEmployeurEtablissement",
    "activitePrincipaleNAF25Etablissement",
]

# From StockUniteLegale — joined on siren to get the legal company name
UNITE_LEGALE_COLS = [
    "siren",
    "denominationUniteLegale",          # company name (SA, SARL, SAS…)
    "nomUniteLegale",                   # surname (for auto-entrepreneurs)
    "prenomUsuelUniteLegale",           # first name (auto-entrepreneurs)
    "sigleUniteLegale",                 # acronym
    "categorieJuridiqueUniteLegale",    # legal category code
    "etatAdministratifUniteLegale",     # A=active, C=ceased
]

# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — FIND SIRENE FILES ON DATA.GOUV.FR
# ══════════════════════════════════════════════════════════════════════════════

def fetch_dataset_resources(slug: str) -> list[dict]:
    """Returns all resources for a data.gouv.fr dataset."""
    url = f"{DATAGOUV_API}/datasets/{slug}/"
    log.info(f"Fetching dataset metadata from data.gouv.fr…")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("resources", [])


def find_best_resource(resources: list[dict], keyword: str, prefer_parquet: bool = True) -> Optional[dict]:
    """
    From a list of resources, finds the most recent file matching `keyword`.
    Prefers Parquet over ZIP/CSV for speed.
    Uses word-boundary matching so 'StockEtablissement' won't match
    'StockEtablissementLiensSuccession'.
    """
    def _title_matches(title: str, kw: str) -> bool:
        t = title.lower()
        k = kw.lower()
        idx = t.find(k)
        if idx == -1:
            return False
        after = t[idx + len(k):]
        # Reject if the keyword is immediately followed by more word chars
        if after and (after[0].isalpha() or after[0] == '_'):
            return False
        return True

    candidates = [
        r for r in resources
        if _title_matches(r.get("title", ""), keyword)
        and r.get("url")
        and not r.get("title", "").lower().startswith("historique")
    ]
    if not candidates:
        return None

    if prefer_parquet:
        parquet = [r for r in candidates if r.get("format", "").lower() == "parquet"
                   or r["url"].endswith(".parquet")]
        if parquet:
            return sorted(parquet, key=lambda r: r.get("last_modified", ""), reverse=True)[0]

    # Fall back to ZIP (contains CSV)
    zips = [r for r in candidates if r.get("format", "").lower() in ("zip", "csv")
            or r["url"].endswith((".zip", ".csv"))]
    return sorted(zips, key=lambda r: r.get("last_modified", ""), reverse=True)[0] if zips else candidates[0]


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — DOWNLOAD WITH CACHE
# ══════════════════════════════════════════════════════════════════════════════

def download_with_cache(resource: dict, cache_dir: Path, max_retries: int = 5) -> Path:
    """
    Downloads a file from data.gouv.fr, using a local cache.
    Re-downloads only if the remote file is newer (checked via last_modified).
    Uses a .part file for safe caching and supports retries on connection drop.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    url        = resource["url"]
    title      = resource.get("title", "unknown").replace("/", "_")
    modified   = resource.get("last_modified", "")
    cache_key  = f"{title}_{modified[:10]}"  # date-stamped filename
    ext        = ".parquet" if url.endswith(".parquet") else ".zip" if url.endswith(".zip") else ".csv"
    local_path = cache_dir / (cache_key + ext)
    part_path  = local_path.with_suffix(ext + ".part")

    if local_path.exists():
        log.info(f"  ✓ Using cached file: {local_path.name}")
        return local_path

    log.info(f"  ↓ Downloading: {title} ({resource.get('filesize', '?')} bytes) …")
    
    for attempt in range(max_retries):
        try:
            with requests.get(url, stream=True, timeout=90) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", 0))
                with open(part_path, "wb") as f, tqdm(
                    total=total, unit="B", unit_scale=True, desc=local_path.name
                ) as bar:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
            
            # If complete without exception, move to final path
            part_path.rename(local_path)
            return local_path
            
        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠ Download interrupted (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                log.info("  Retrying in 10 seconds...")
                time.sleep(10)
            else:
                log.error("  ❌ Download failed after max retries.")
                raise e

    return local_path


def load_parquet_or_zip(local_path: Path, columns: list[str]) -> pl.LazyFrame:
    """
    Loads a Parquet or ZIP/CSV file into a Polars LazyFrame.
    Only reads the requested columns.
    """
    if local_path.suffix == ".parquet":
        log.info(f"  Reading Parquet: {local_path.name}")
        return pl.scan_parquet(local_path, n_rows=None)

    elif local_path.suffix == ".zip":
        log.info(f"  Extracting ZIP: {local_path.name}")
        with zipfile.ZipFile(local_path) as zf:
            csvs = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csvs:
                raise ValueError(f"No CSV found inside {local_path.name}")
            csv_name = csvs[0]
            log.info(f"    → Found CSV: {csv_name}")
            with zf.open(csv_name) as f:
                # Read in chunks using Polars (lazy from buffer)
                data = f.read()
        return pl.read_csv(
            BytesIO(data),
            infer_schema_length=10000,
            ignore_errors=True,
        ).lazy()

    else:
        return pl.scan_csv(local_path, infer_schema_length=10000, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — FILTER & JOIN
# ══════════════════════════════════════════════════════════════════════════════

def filter_btp(lf: pl.LazyFrame, active_only: bool = False, depts: list[str] = None) -> pl.DataFrame:
    """
    Filters a SIRENE établissement LazyFrame to BTP companies only.
    """
    log.info("  Filtering NAF 41/42/43…")
    expr = (
        pl.col("activitePrincipaleEtablissement")
        .str.slice(0, 2)
        .is_in(list(NAF_PREFIXES))
    )

    lf = lf.filter(expr)

    if active_only:
        log.info("  Keeping active establishments only (etatAdministratif = A)…")
        lf = lf.filter(pl.col("etatAdministratifEtablissement") == "A")

    if depts:
        log.info(f"  Filtering to département(s): {depts}")
        dept_expr = pl.col("codePostalEtablissement").str.slice(0, 2).is_in(depts)
        lf = lf.filter(dept_expr)

    # Select only the columns we want (ignore missing ones)
    available = set(lf.schema.names())
    select_cols = [c for c in ETABLISSEMENT_COLS if c in available]
    lf = lf.select(select_cols)

    log.info("  Collecting results (this may take a few minutes)…")
    df = lf.collect()
    log.info(f"  → {len(df):,} BTP establishments found")
    return df


def build_company_name(ul: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a unified `nom_entreprise` column from StockUniteLegale.
    For legal entities: denominationUniteLegale
    For individuals: prenomUsuelUniteLegale + nomUniteLegale
    """
    ul = ul.with_columns(
        pl.when(pl.col("denominationUniteLegale").is_not_null() & (pl.col("denominationUniteLegale") != ""))
        .then(pl.col("denominationUniteLegale"))
        .when(pl.col("prenomUsuelUniteLegale").is_not_null())
        .then(
            pl.concat_str([
                pl.col("prenomUsuelUniteLegale").fill_null(""),
                pl.lit(" "),
                pl.col("nomUniteLegale").fill_null(""),
            ])
        )
        .otherwise(pl.col("nomUniteLegale"))
        .alias("nom_entreprise")
    )
    return ul


def join_unite_legale(etab_df: pl.DataFrame, ul_df: pl.DataFrame) -> pl.DataFrame:
    """Joins établissement data with unité légale data on siren."""
    log.info("  Joining with StockUniteLegale for company names…")
    ul_clean = build_company_name(ul_df)

    # Keep only relevant columns
    keep = ["siren", "nom_entreprise", "sigleUniteLegale",
            "categorieJuridiqueUniteLegale", "etatAdministratifUniteLegale"]
    keep = [c for c in keep if c in ul_clean.columns]
    ul_clean = ul_clean.select(keep)

    df = etab_df.join(ul_clean, on="siren", how="left")

    # Move nom_entreprise to front
    cols = ["siren", "nic", "siret", "nom_entreprise"] + [
        c for c in df.columns if c not in ("siren", "nic", "siret", "nom_entreprise")
    ]
    cols = [c for c in cols if c in df.columns]
    return df.select(cols)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — GEOCODING (Lambert 93 → WGS84)
# ══════════════════════════════════════════════════════════════════════════════

def add_wgs84_coords(df: pl.DataFrame) -> pl.DataFrame:
    """
    Converts Lambert93 (EPSG:2154) coordinates to WGS84 (latitude/longitude).
    Adds columns: latitude, longitude, geolocalisation
    """
    if not HAS_PYPROJ:
        log.warning("  pyproj not available — skipping Lambert→WGS84 conversion")
        df = df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("latitude"),
            pl.lit(None).cast(pl.Float64).alias("longitude"),
            pl.lit(None).cast(pl.Utf8).alias("geolocalisation"),
        ])
        return df

    x_col = "coordonneeLambertAbscisseEtablissement"
    y_col = "coordonneeLambertOrdonneeEtablissement"

    if x_col not in df.columns or y_col not in df.columns:
        log.warning("  Lambert coordinates not found in data — skipping geocoding")
        return df

    log.info("  Converting Lambert93 → WGS84…")
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

    x_vals = df[x_col].cast(pl.Float64, strict=False).to_list()
    y_vals = df[y_col].cast(pl.Float64, strict=False).to_list()

    lons, lats = [], []
    for x, y in zip(x_vals, y_vals):
        if x is not None and y is not None:
            try:
                lon, lat = transformer.transform(x, y)
                lons.append(round(lon, 7))
                lats.append(round(lat, 7))
            except Exception:
                lons.append(None)
                lats.append(None)
        else:
            lons.append(None)
            lats.append(None)

    df = df.with_columns([
        pl.Series("longitude", lons, dtype=pl.Float64),
        pl.Series("latitude",  lats, dtype=pl.Float64),
    ])

    # Combined geolocalisation string: "lat,lon"
    df = df.with_columns(
        pl.when(pl.col("latitude").is_not_null())
        .then(
            pl.concat_str([
                pl.col("latitude").cast(pl.Utf8),
                pl.lit(","),
                pl.col("longitude").cast(pl.Utf8),
            ])
        )
        .otherwise(pl.lit(None))
        .alias("geolocalisation")
    )
    log.info(f"  → {df['latitude'].drop_nulls().len():,} rows geocoded")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 5 — ENRICHMENT: Annuaire des Entreprises API
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_annuaire_one(client: httpx.AsyncClient, siret: str, nom: str = "", cp: str = "") -> dict:
    """
    Fetches enrichment data for one SIRET via Annuaire des Entreprises search API.
    Tries SIRET lookup first, falls back to name + postal code search.
    Returns: website, email, phone, latitude, longitude, geo_adresse, dirigeant_principal, est_rge
    """
    empty = {
        "siret": siret, "website": "", "email": "", "phone": "",
        "_ann_latitude": None, "_ann_longitude": None,
        "_ann_geo_adresse": "", "dirigeant_principal": "",
        "est_rge": "", "source": "",
    }
    try:
        # Search by SIRET (most precise)
        query = siret if siret else f"{nom} {cp}".strip()
        url = f"{ANNUAIRE_SEARCH_API}?q={query}&page=1&per_page=1"
        r = await client.get(url, timeout=10)
        if r.status_code != 200:
            return empty
        data = r.json()
        results = data.get("results", [])
        if not results:
            return empty

        company = results[0]
        siege = company.get("siege") or {}
        complements = company.get("complements") or {}
        dirigeants = company.get("dirigeants") or []

        # Primary dirigeant
        dir_name = ""
        if dirigeants:
            d = dirigeants[0]
            if d.get("type_dirigeant") == "personne physique":
                dir_name = f"{d.get('prenoms', '')} {d.get('nom', '')}".strip()
            else:
                dir_name = d.get("denomination", "")

        # RGE certification (very useful for BTP prospecting)
        est_rge = "Oui" if complements.get("est_rge") else ""

        # Contact info — not publicly available via this API
        # phone/email/website require Pappers or Google Places
        return {
            "siret":             siret,
            "website":           "",      # not in public API
            "email":             "",      # not in public API
            "phone":             "",      # not in public API
            "_ann_latitude":     siege.get("latitude"),
            "_ann_longitude":    siege.get("longitude"),
            "_ann_geo_adresse":  siege.get("geo_adresse", ""),
            "dirigeant_principal": dir_name,
            "est_rge":           est_rge,
            "source":            "recherche-entreprises.api.gouv.fr",
        }
    except Exception:
        pass
    return empty


async def enrich_batch(rows: list[dict], semaphore: asyncio.Semaphore) -> list[dict]:
    """Enriches a batch of rows (siret, nom, cp) with Annuaire data."""
    results = []
    async with httpx.AsyncClient(
        headers={"User-Agent": "btp-db-builder/1.0 (contact: opensource)"},
        follow_redirects=True,
    ) as client:
        for i, row in enumerate(rows):
            async with semaphore:
                result = await fetch_annuaire_one(
                    client,
                    siret=row.get("siret", ""),
                    nom=row.get("nom", ""),
                    cp=row.get("cp", ""),
                )
                results.append(result)
                if i > 0 and i % 200 == 0:
                    log.info(f"    Enriched {i:,}/{len(rows):,} SIRETs…")
                await asyncio.sleep(ENRICH_DELAY)
    return results


def enrich_with_annuaire(df: pl.DataFrame, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Adds enrichment columns via the Annuaire des Entreprises API:
      - dirigeant_principal: main manager name (useful for prospecting)
      - est_rge: RGE certification (relevant for BTP)
      - _ann_latitude / _ann_longitude: from Annuaire (may be more accurate than Lambert conv.)
      - website / email / phone: NOT available in public French APIs — left empty
        → To fill these, use: Pappers API (https://www.pappers.fr/api) or Google Places

    Rate: ~5 req/s → 100k rows ≈ 5h30. Use --limit N to test first.
    """
    rows_to_enrich = df.select(
        ["siret",
         pl.col("nom_entreprise").alias("nom"),
         pl.col("codePostalEtablissement").alias("cp")]
    ).to_dicts()

    if limit:
        rows_to_enrich = rows_to_enrich[:limit]
        log.info(f"  Enriching first {limit:,} rows (test mode)…")
    else:
        log.info(f"  Enriching {len(rows_to_enrich):,} SIRETs via Annuaire des Entreprises…")
        log.info("  ⏱  ~5 req/s. Use --limit N to test on a subset first.")

    semaphore = asyncio.Semaphore(ENRICH_CONCURRENT)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(enrich_batch(rows_to_enrich, semaphore))
    loop.close()

    enrichment_df = pl.DataFrame(results, infer_schema_length=100)

    # Merge back
    df = df.join(enrichment_df, on="siret", how="left")

    # If Annuaire lat/lon is better (non-null) and Lambert was null, use Annuaire's
    if "_ann_latitude" in df.columns and "latitude" in df.columns:
        df = df.with_columns([
            pl.when(pl.col("latitude").is_null() & pl.col("_ann_latitude").is_not_null())
            .then(pl.col("_ann_latitude"))
            .otherwise(pl.col("latitude"))
            .alias("latitude"),
            pl.when(pl.col("longitude").is_null() & pl.col("_ann_longitude").is_not_null())
            .then(pl.col("_ann_longitude"))
            .otherwise(pl.col("longitude"))
            .alias("longitude"),
        ]).drop(["_ann_latitude", "_ann_longitude"])

    rge_count = enrichment_df.filter(pl.col("est_rge") == "Oui").height
    log.info(f"  → Done. {rge_count:,} RGE-certified companies found")
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 6 — GOOGLE PLACES (optional — requires API key)
# ══════════════════════════════════════════════════════════════════════════════

def enrich_with_google_places(df: pl.DataFrame, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Adds google_reviews via Google Places Text Search API.
    Requires: GOOGLE_PLACES_API_KEY environment variable.

    Pricing: ~$17 per 1,000 requests (Text Search)
    Set your key: export GOOGLE_PLACES_API_KEY=your_key_here
    """
    if not GOOGLE_PLACES_API_KEY:
        log.warning("  GOOGLE_PLACES_API_KEY not set — skipping Google reviews")
        df = df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("google_reviews"),
            pl.lit(None).cast(pl.Utf8).alias("other_reviews"),
        )
        return df

    log.info("  Fetching Google Places reviews…")
    results = {}
    rows_to_enrich = df.select(["siret", "nom_entreprise", "codePostalEtablissement",
                                "libelleCommuneEtablissement"]).to_dicts()

    if limit:
        rows_to_enrich = rows_to_enrich[:limit]

    for i, row in enumerate(tqdm(rows_to_enrich, desc="Google Places")):
        name    = row.get("nom_entreprise") or ""
        city    = row.get("libelleCommuneEtablissement") or ""
        cp      = row.get("codePostalEtablissement") or ""
        query   = f"{name} {city} {cp}".strip()
        if not query:
            continue

        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {"query": query, "key": GOOGLE_PLACES_API_KEY, "language": "fr"}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            if data.get("results"):
                place = data["results"][0]
                rating      = place.get("rating", "")
                n_ratings   = place.get("user_ratings_total", "")
                results[row["siret"]] = f"{rating}/5 ({n_ratings} avis)" if rating else ""
        except Exception:
            pass
        time.sleep(GOOGLE_DELAY)

    df = df.with_columns([
        pl.col("siret").map_elements(lambda s: results.get(s, ""), return_dtype=pl.Utf8).alias("google_reviews"),
        pl.lit(None).cast(pl.Utf8).alias("other_reviews"),
    ])
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 7 — FINAL COLUMN ORDERING & EXPORT
# ══════════════════════════════════════════════════════════════════════════════

FINAL_COLUMN_ORDER = [
    "siren", "nic", "siret", "nom_entreprise",
    "statutDiffusionEtablissement", "dateCreationEtablissement",
    "trancheEffectifsEtablissement", "anneeEffectifsEtablissement",
    "activitePrincipaleRegistreMetiersEtablissement",
    "dateDernierTraitementEtablissement", "etablissementSiege",
    "nombrePeriodesEtablissement",
    "complementAdresseEtablissement", "numeroVoieEtablissement",
    "indiceRepetitionEtablissement", "dernierNumeroVoieEtablissement",
    "indiceRepetitionDernierNumeroVoieEtablissement",
    "typeVoieEtablissement", "libelleVoieEtablissement",
    "codePostalEtablissement", "libelleCommuneEtablissement",
    "libelleCommuneEtrangerEtablissement", "distributionSpecialeEtablissement",
    "codeCommuneEtablissement", "codeCedexEtablissement", "libelleCedexEtablissement",
    "codePaysEtrangerEtablissement", "libellePaysEtrangerEtablissement",
    "identifiantAdresseEtablissement",
    "coordonneeLambertAbscisseEtablissement", "coordonneeLambertOrdonneeEtablissement",
    "complementAdresse2Etablissement", "numeroVoie2Etablissement",
    "indiceRepetition2Etablissement", "typeVoie2Etablissement",
    "libelleVoie2Etablissement", "codePostal2Etablissement",
    "libelleCommune2Etablissement", "libelleCommuneEtranger2Etablissement",
    "distributionSpeciale2Etablissement", "codeCommune2Etablissement",
    "codeCedex2Etablissement", "libelleCedex2Etablissement",
    "codePaysEtranger2Etablissement", "libellePaysEtranger2Etablissement",
    "dateDebut", "etatAdministratifEtablissement",
    "enseigne1Etablissement", "enseigne2Etablissement", "enseigne3Etablissement",
    "denominationUsuelleEtablissement",
    "activitePrincipaleEtablissement", "nomenclatureActivitePrincipaleEtablissement",
    "caractereEmployeurEtablissement", "activitePrincipaleNAF25Etablissement",
    # Legal unit fields (joined)
    "sigleUniteLegale", "categorieJuridiqueUniteLegale", "etatAdministratifUniteLegale",
    # Enriched fields
    "dirigeant_principal",    # from Annuaire des Entreprises (useful for prospecting)
    "est_rge",                # RGE certification (renewable energy / eco-renovation)
    "website", "email", "phone",
    "geolocalisation", "longitude", "latitude",
    "google_reviews", "other_reviews",
    "source", "sources",
]


def finalise(df: pl.DataFrame) -> pl.DataFrame:
    """Reorders columns, fills missing enriched cols with null, adds source metadata."""
    # Add empty enriched columns if missing
    for col in ["website", "email", "phone", "google_reviews", "other_reviews",
                "source", "sources", "geolocalisation", "longitude", "latitude",
                "dirigeant_principal", "est_rge"]:
        if col not in df.columns:
            if col in ("longitude", "latitude"):
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    # Add data sources metadata
    df = df.with_columns(
        pl.when(pl.col("source").is_null() | (pl.col("source") == ""))
        .then(pl.lit("INSEE SIRENE"))
        .otherwise(pl.col("source"))
        .alias("source")
    )
    df = df.with_columns(
        pl.lit("INSEE SIRENE (data.gouv.fr)").alias("sources")
    )

    # Reorder columns (keep only present ones, in order)
    ordered = [c for c in FINAL_COLUMN_ORDER if c in df.columns]
    extra   = [c for c in df.columns if c not in ordered]
    return df.select(ordered + extra)


def export_csv(df: pl.DataFrame, path: Path):
    """Exports the DataFrame to CSV."""
    log.info(f"  Writing {len(df):,} rows to {path}…")
    df.write_csv(path)
    size_mb = path.stat().st_size / (1024 * 1024)
    log.info(f"  ✓ Saved: {path.name} ({size_mb:.1f} MB)")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="Build a database of French BTP companies from SIRENE + enrichment sources."
    )
    parser.add_argument("--enrich", action="store_true",
                        help="Enable Annuaire des Entreprises API enrichment (slow)")
    parser.add_argument("--google", action="store_true",
                        help="Enable Google Places API (requires GOOGLE_PLACES_API_KEY env var)")
    parser.add_argument("--active-only", action="store_true",
                        help="Keep only active establishments")
    parser.add_argument("--dept", type=str, default="",
                        help="Comma-separated département codes to filter (e.g. 75,69,13)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit enrichment to first N rows (for testing)")
    parser.add_argument("--output", type=str, default=str(OUTPUT_FILE),
                        help="Output CSV path")
    parser.add_argument("--no-cache", action="store_true",
                        help="Force re-download even if cached files exist")
    return parser.parse_args()


def main():
    args = parse_args()
    depts = [d.strip().zfill(2) for d in args.dept.split(",") if d.strip()] if args.dept else []
    output_path = Path(args.output)

    print("\n" + "═" * 70)
    print("  BTP COMPANIES DATABASE BUILDER")
    print(f"  Sectors: NAF 41 / 42 / 43")
    print(f"  Scope: {'Active only' if args.active_only else 'All (active + closed)'}")
    print(f"  Geography: {', '.join(depts) if depts else 'All of France'}")
    print(f"  Enrichment: {'Annuaire + Google' if args.google else 'Annuaire' if args.enrich else 'SIRENE only'}")
    print(f"  Output: {output_path}")
    print("═" * 70 + "\n")

    # ── 1. Get SIRENE file URLs ───────────────────────────────────────────────
    log.info("STEP 1/6 — Fetching SIRENE dataset metadata…")
    resources = fetch_dataset_resources(SIRENE_SLUG)

    etab_resource = find_best_resource(resources, "StockEtablissement", prefer_parquet=True)
    ul_resource   = find_best_resource(resources, "StockUniteLegale",   prefer_parquet=True)

    if not etab_resource:
        log.error("Could not find StockEtablissement resource on data.gouv.fr")
        sys.exit(1)
    if not ul_resource:
        log.error("Could not find StockUniteLegale resource on data.gouv.fr")
        sys.exit(1)

    log.info(f"  Établissements : {etab_resource['title']} ({etab_resource.get('format','?')})")
    log.info(f"  Unités légales : {ul_resource['title']} ({ul_resource.get('format','?')})")

    # ── 2. Download ───────────────────────────────────────────────────────────
    cache = CACHE_DIR if not args.no_cache else Path("/tmp/sirene_nocache")
    log.info("\nSTEP 2/6 — Downloading SIRENE files…")
    etab_path = download_with_cache(etab_resource, cache)
    ul_path   = download_with_cache(ul_resource,   cache)

    # ── 3. Load & filter établissements ──────────────────────────────────────
    log.info("\nSTEP 3/6 — Loading and filtering établissements (NAF 41/42/43)…")
    etab_lf = load_parquet_or_zip(etab_path, ETABLISSEMENT_COLS)
    etab_df = filter_btp(etab_lf, active_only=args.active_only, depts=depts)

    # ── 4. Join with unité légale for company names ───────────────────────────
    log.info("\nSTEP 4/6 — Loading unité légale & joining company names…")
    ul_lf = load_parquet_or_zip(ul_path, UNITE_LEGALE_COLS)
    # Only load UL rows matching our SIRENs (memory optimization)
    our_sirens = etab_df["siren"].unique().to_list()
    ul_df = ul_lf.filter(pl.col("siren").is_in(our_sirens)).collect()
    log.info(f"  Loaded {len(ul_df):,} matching unités légales")
    etab_df = join_unite_legale(etab_df, ul_df)
    del ul_df  # free memory

    # ── 5. Geocoding ──────────────────────────────────────────────────────────
    log.info("\nSTEP 5/6 — Geocoding (Lambert93 → WGS84)…")
    etab_df = add_wgs84_coords(etab_df)

    # ── 6. Optional enrichment ────────────────────────────────────────────────
    log.info("\nSTEP 6/6 — Enrichment…")
    if args.enrich:
        etab_df = enrich_with_annuaire(etab_df, limit=args.limit)
    else:
        log.info("  Skipping Annuaire API (use --enrich to enable)")
        for col in ["website", "email", "phone", "source"]:
            etab_df = etab_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

    if args.google:
        etab_df = enrich_with_google_places(etab_df, limit=args.limit)
    else:
        log.info("  Skipping Google Places (use --google + GOOGLE_PLACES_API_KEY to enable)")

    # ── Finalise & export ─────────────────────────────────────────────────────
    log.info("\nFinalising columns and exporting…")
    etab_df = finalise(etab_df)
    export_csv(etab_df, output_path)

    print("\n" + "═" * 70)
    print(f"  ✅  DONE — {len(etab_df):,} BTP establishments exported")
    print(f"  File: {output_path}")
    if not args.enrich:
        print("\n  ℹ️  To enrich with website/email/phone, re-run with:")
        print(f"     python {Path(__file__).name} --enrich")
    if not args.google:
        print("\n  ℹ️  To add Google reviews, run:")
        print(f"     export GOOGLE_PLACES_API_KEY=your_key_here")
        print(f"     python {Path(__file__).name} --google --limit 1000  # test first!")
    print("═" * 70 + "\n")


if __name__ == "__main__":
    main()
