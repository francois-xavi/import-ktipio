#!/usr/bin/env python3
"""
Enrichissement Qualibat / RGE direct depuis Neon PostgreSQL

Usage:
    # Mode AUDIT : juste afficher les stats, pas de mise à jour
    python enrich_qualibat_rge_db.py --audit

    # Mode UPDATE : mettre à jour la DB
    python enrich_qualibat_rge_db.py --update

    # Mode AUDIT + export CSV des entreprises sans qualifications
    python enrich_qualibat_rge_db.py --audit --export-missing

    # Forcer le re-téléchargement (sinon utilise cache si <24h)
    python enrich_qualibat_rge_db.py --update --refresh
"""

import os
import sys
import time
import argparse
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd
import psycopg2
import psycopg2.extras


# ─── CONFIG ───────────────────────────────────────────────────────────────────

DB_URL = os.getenv("NEON_DATABASE_URL")
if not DB_URL:
    print("❌ ERROR: NEON_DATABASE_URL non définie")
    sys.exit(1)

RGE_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/lines"
CACHE_FILE = Path("rge_cache.json")
CACHE_MAX_AGE = 24 * 3600  # 24h

# Colonnes du fichier RGE ADEME
COL_SIRET   = "siret"
COL_ORG     = "organisme"
COL_QUALIF  = "nom_qualification"
COL_DATE    = "lien_date_fin"
COL_NOM     = "nom_entreprise"
COL_EMAIL   = "email"
COL_TEL     = "telephone"
COL_SITE    = "site_internet"
COL_CERT    = "nom_certificat"
COL_DOMAINE = "domaine"


# ─── DOWNLOAD RGE ─────────────────────────────────────────────────────────────

def download_rge(use_cache: bool = True) -> pd.DataFrame:
    """Télécharge le fichier RGE depuis l'API ADEME, avec cache 24h."""
    if use_cache and CACHE_FILE.exists():
        age = time.time() - CACHE_FILE.stat().st_mtime
        if age < CACHE_MAX_AGE:
            print(f"📂 Utilisation du cache (âge: {age/3600:.1f}h)")
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return pd.DataFrame(json.load(f))

    print("📥 Téléchargement du fichier RGE depuis l'API ADEME...")
    r = requests.get(RGE_URL, params={"size": 1, "sort": "_id"}, timeout=30)
    total_api = r.json().get("total", 0)
    print(f"   Total entreprises RGE : {total_api:,}")

    all_rge = []
    params = {"size": 10000, "sort": "_id"}
    after = None

    while True:
        try:
            if after:
                params["after"] = after

            response = requests.get(RGE_URL, params=params, timeout=60)

            if response.status_code == 400:
                params["size"] = 1000
                response = requests.get(RGE_URL, params=params, timeout=60)

            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            if not results:
                break

            all_rge.extend(results)
            print(f"   Récupéré {len(all_rge):,} / {total_api:,}...")

            next_url = data.get("next")
            if not next_url:
                break

            qs = parse_qs(urlparse(next_url).query)
            after = qs.get("after", [None])[0]
            if not after:
                break

            if len(all_rge) >= total_api:
                break

            time.sleep(0.15)

        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur téléchargement: {e}")
            break

    # Sauvegarder le cache
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(all_rge, f, ensure_ascii=False)
    print(f"💾 Cache sauvegardé : {CACHE_FILE}")

    return pd.DataFrame(all_rge)


# ─── DETECTION QUALIBAT ROBUSTE ──────────────────────────────────────────────

def is_qualibat_row(row) -> bool:
    """
    Détection Qualibat plus robuste : cherche dans organisme, certificat
    et qualification (variantes : Qualibat, QUALIBAT, Qualibat-RGE, etc.)
    """
    fields = [
        str(row.get(COL_ORG, "")),
        str(row.get(COL_CERT, "")),
        str(row.get(COL_QUALIF, "")),
    ]
    text = " ".join(fields).lower()
    return "qualibat" in text


# ─── DB OPERATIONS ────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DB_URL, connect_timeout=15)


def fetch_companies_status(conn) -> pd.DataFrame:
    """Récupère le status actuel de toutes les entreprises."""
    print("\n📦 Lecture de la base Neon...")
    query = """
        SELECT
            siret,
            siren,
            raison_sociale,
            is_qualibat,
            is_rge,
            nb_qualifications_rge,
            nb_qualifications_qualibat,
            rge_organisme
        FROM companies
        WHERE siret IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    print(f"   {len(df):,} entreprises dans la DB")
    return df


def update_companies_qualifications(conn, df_updates: pd.DataFrame, batch_size: int = 500):
    """Met à jour la DB avec les nouvelles qualifications."""
    print(f"\n🔄 Mise à jour de {len(df_updates):,} entreprises...")

    cur = conn.cursor()
    total = len(df_updates)
    updated = 0

    for i in range(0, total, batch_size):
        batch = df_updates.iloc[i:i + batch_size]

        for _, row in batch.iterrows():
            try:
                cur.execute("""
                    UPDATE companies
                    SET
                        is_qualibat                 = %s,
                        is_rge                      = %s,
                        nb_qualifications_rge       = %s,
                        nb_qualifications_qualibat  = %s,
                        rge_organisme               = %s,
                        rge_qualifications          = %s,
                        rge_certificats             = %s,
                        rge_domaines                = %s,
                        rge_date_fin                = %s,
                        rge_email                   = COALESCE(rge_email, %s),
                        rge_telephone               = COALESCE(rge_telephone, %s),
                        rge_site_internet           = COALESCE(rge_site_internet, %s)
                    WHERE siret = %s
                """, (
                    bool(row.get("is_qualibat", False)),
                    bool(row.get("is_rge", False)),
                    int(row.get("nb_qualifications_rge", 0)),
                    int(row.get("nb_qualifications_qualibat", 0)),
                    row.get("rge_organisme"),
                    row.get("rge_qualifications"),
                    row.get("rge_certificats"),
                    row.get("rge_domaines"),
                    row.get("rge_date_fin"),
                    row.get("rge_email"),
                    row.get("rge_telephone"),
                    row.get("rge_site_internet"),
                    row["siret"],
                ))
                updated += 1
            except Exception as e:
                print(f"  ⚠️  Erreur SIRET {row['siret']}: {e}")
                conn.rollback()
                cur = conn.cursor()
                continue

        conn.commit()
        print(f"   {min(i + batch_size, total):,}/{total:,} mises à jour...")

    cur.close()
    print(f"✅ {updated:,} entreprises mises à jour")
    return updated


# ─── AGGREGATE RGE BY SIRET ───────────────────────────────────────────────────

def aggregate_rge(df_rge: pd.DataFrame) -> pd.DataFrame:
    """Agrège les qualifications par SIRET avec détection Qualibat robuste."""
    df_rge[COL_SIRET] = df_rge[COL_SIRET].astype(str).str.zfill(14)
    df_rge["siren"] = df_rge[COL_SIRET].str[:9]

    # Détection Qualibat (robuste, sur 3 colonnes)
    df_rge["is_qualibat_row"] = df_rge.apply(is_qualibat_row, axis=1)

    print(f"\n📋 Organismes dans le fichier RGE :")
    print(df_rge[COL_ORG].value_counts().head(20).to_string())

    print(f"\n📊 Lignes Qualibat détectées : {df_rge['is_qualibat_row'].sum():,}")

    df_agg = df_rge.groupby(COL_SIRET).agg(
        siren                       = ("siren", "first"),
        rge_nom_entreprise          = (COL_NOM, "first"),
        rge_organisme               = (COL_ORG, lambda x: ", ".join(sorted(set(x.dropna())))),
        rge_qualifications          = (COL_QUALIF, lambda x: "; ".join(sorted(set(x.dropna())))),
        rge_certificats             = (COL_CERT, lambda x: ", ".join(sorted(set(x.dropna())))),
        rge_domaines                = (COL_DOMAINE, lambda x: ", ".join(sorted(set(x.dropna())))),
        rge_date_fin                = (COL_DATE, "max"),
        rge_email                   = (COL_EMAIL, "first"),
        rge_telephone               = (COL_TEL, "first"),
        rge_site_internet           = (COL_SITE, "first"),
        nb_qualifications_rge       = (COL_SIRET, "count"),
        nb_qualifications_qualibat  = ("is_qualibat_row", "sum"),
    ).reset_index().rename(columns={COL_SIRET: "siret"})

    df_agg["is_qualibat"] = df_agg["nb_qualifications_qualibat"] > 0
    df_agg["is_rge"] = df_agg["nb_qualifications_rge"] > 0

    print(f"✅ {len(df_agg):,} entreprises uniques après agrégation")
    print(f"   - is_qualibat : {df_agg['is_qualibat'].sum():,}")
    print(f"   - is_rge      : {df_agg['is_rge'].sum():,}")

    return df_agg


# ─── AUDIT ────────────────────────────────────────────────────────────────────

def audit(df_db: pd.DataFrame, df_rge_agg: pd.DataFrame, export_missing: bool = False):
    """
    Compare la DB actuelle avec le fichier RGE et identifie les écarts.
    """
    print("\n" + "=" * 70)
    print("  AUDIT QUALIBAT / RGE")
    print("=" * 70)

    # Normaliser sirets
    df_db["siret"] = df_db["siret"].astype(str).str.zfill(14)
    df_rge_agg["siret"] = df_rge_agg["siret"].astype(str).str.zfill(14)

    # Stats DB actuelles
    db_total = len(df_db)
    db_qualibat = df_db["is_qualibat"].fillna(False).sum()
    db_rge = df_db["is_rge"].fillna(False).sum()

    print(f"\n📊 État ACTUEL de la DB :")
    print(f"   Total entreprises  : {db_total:,}")
    print(f"   is_qualibat = TRUE : {db_qualibat:,}  ({db_qualibat/db_total*100:.1f}%)")
    print(f"   is_rge = TRUE      : {db_rge:,}  ({db_rge/db_total*100:.1f}%)")

    # Merge pour comparer
    merged = df_db.merge(
        df_rge_agg[["siret", "is_qualibat", "is_rge",
                    "nb_qualifications_rge", "nb_qualifications_qualibat"]],
        on="siret", how="left", suffixes=("_db", "_rge")
    )

    # Entreprises qui DEVRAIENT être Qualibat mais ne le sont pas
    should_be_qualibat = merged[
        (merged["is_qualibat_rge"] == True) &
        (merged["is_qualibat_db"].fillna(False) == False)
    ]

    # Entreprises qui DEVRAIENT être RGE mais ne le sont pas
    should_be_rge = merged[
        (merged["is_rge_rge"] == True) &
        (merged["is_rge_db"].fillna(False) == False)
    ]

    # Entreprises Qualibat dans la DB mais pas dans le RGE
    in_db_not_rge_qualibat = merged[
        (merged["is_qualibat_db"] == True) &
        (merged["is_qualibat_rge"].fillna(False) == False)
    ]

    print(f"\n⚠️  ÉCARTS DÉTECTÉS :")
    print(f"   Devraient être Qualibat (selon RGE) : {len(should_be_qualibat):,}")
    print(f"   Devraient être RGE (selon RGE)      : {len(should_be_rge):,}")
    print(f"   Qualibat en DB mais absents du RGE  : {len(in_db_not_rge_qualibat):,}")
    print(f"     (peuvent être des Qualibat classiques non-RGE)")

    # Entreprises sans qualifications du tout
    no_qual = df_db[
        (df_db["is_qualibat"].fillna(False) == False) &
        (df_db["is_rge"].fillna(False) == False)
    ]
    print(f"\n📭 Entreprises sans aucune qualification : {len(no_qual):,}")

    if export_missing:
        # Export des entreprises à mettre à jour
        out_file = "qualibat_rge_a_mettre_a_jour.csv"
        cols = ["siret", "siren", "raison_sociale",
                "is_qualibat_db", "is_qualibat_rge",
                "is_rge_db", "is_rge_rge",
                "nb_qualifications_rge", "nb_qualifications_qualibat"]

        to_export = pd.concat([should_be_qualibat, should_be_rge]).drop_duplicates(subset="siret")
        to_export[cols].to_csv(out_file, index=False)
        print(f"\n💾 Export : {out_file} ({len(to_export):,} entreprises à mettre à jour)")

        # Export des entreprises sans qualif (pour vérification manuelle)
        out_file_no_qual = "entreprises_sans_qualifications.csv"
        no_qual[["siret", "siren", "raison_sociale"]].head(50000).to_csv(
            out_file_no_qual, index=False
        )
        print(f"💾 Export : {out_file_no_qual} ({len(no_qual):,} entreprises)")

    return should_be_qualibat, should_be_rge


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Enrichissement Qualibat/RGE direct DB")
    p.add_argument("--audit", action="store_true", help="Mode audit (lecture seule)")
    p.add_argument("--update", action="store_true", help="Mettre à jour la DB")
    p.add_argument("--export-missing", action="store_true",
                   help="Exporter les entreprises à mettre à jour")
    p.add_argument("--refresh", action="store_true",
                   help="Forcer le re-téléchargement du RGE")
    return p.parse_args()


def main():
    args = parse_args()

    if not args.audit and not args.update:
        print("⚠️  Spécifiez --audit ou --update")
        sys.exit(1)

    # 1. Télécharger RGE
    df_rge = download_rge(use_cache=not args.refresh)
    if df_rge.empty:
        print("❌ Aucune donnée RGE récupérée")
        sys.exit(1)

    # 2. Agréger
    df_rge_agg = aggregate_rge(df_rge)

    # 3. Connexion DB
    conn = get_conn()
    df_db = fetch_companies_status(conn)

    # 4. Audit
    audit(df_db, df_rge_agg, export_missing=args.export_missing)

    # 5. Update si demandé
    if args.update:
        print("\n" + "=" * 70)
        print("  MISE À JOUR DE LA DB")
        print("=" * 70)

        # Préparer les updates : seulement les entreprises présentes dans le RGE
        df_db["siret"] = df_db["siret"].astype(str).str.zfill(14)
        df_rge_agg["siret"] = df_rge_agg["siret"].astype(str).str.zfill(14)

        df_to_update = df_rge_agg[df_rge_agg["siret"].isin(set(df_db["siret"]))]
        print(f"\n📋 {len(df_to_update):,} entreprises à mettre à jour")

        confirm = input("\n⚠️  Confirmer la mise à jour ? (oui/non): ").strip().lower()
        if confirm in ("oui", "o", "yes", "y"):
            update_companies_qualifications(conn, df_to_update)
        else:
            print("❌ Annulé")

    conn.close()
    print("\n✅ Terminé")


if __name__ == "__main__":
    main()
