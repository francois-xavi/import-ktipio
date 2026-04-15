import pandas as pd
import requests
import time
from urllib.parse import urlparse, parse_qs

# ─── 1. TÉLÉCHARGEMENT RGE ────────────────────────────────────────────────────
print("📥 Téléchargement du fichier RGE depuis l'API ADEME...")

RGE_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/lines"

# Colonnes réelles confirmées
COL_SIRET  = "siret"
COL_ORG    = "organisme"
COL_QUALIF = "nom_qualification"      # ← corrigé
COL_DATE   = "lien_date_fin"          # ← corrigé
COL_NOM    = "nom_entreprise"
COL_EMAIL  = "email"
COL_TEL    = "telephone"
COL_SITE   = "site_internet"
COL_CERT   = "nom_certificat"
COL_DOMAINE= "domaine"

r = requests.get(RGE_URL, params={"size": 1, "sort": "_id"})
total_api = r.json().get("total", 0)
print(f"   Total entreprises RGE : {total_api:,}")

# ── Pagination par curseur "after" ────────────────────────────────────────────
all_rge = []
params  = {"size": 10000, "sort": "_id"}
after   = None

while True:
    try:
        if after:
            params["after"] = after

        response = requests.get(RGE_URL, params=params)

        if response.status_code == 400:
            print("⚠️  400 reçu, réduction à size=1000...")
            params["size"] = 1000
            response = requests.get(RGE_URL, params=params)

        response.raise_for_status()
        data    = response.json()
        results = data.get("results", [])

        if not results:
            break

        all_rge.extend(results)
        print(f"   Récupéré {len(all_rge):,} / {total_api:,}...")

        # Extraire le curseur depuis l'URL "next"
        next_url = data.get("next")
        if not next_url:
            break

        qs    = parse_qs(urlparse(next_url).query)
        after = qs.get("after", [None])[0]
        if not after:
            break

        if len(all_rge) >= total_api:
            break

        time.sleep(0.15)

    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur : {e}")
        break

df_rge = pd.DataFrame(all_rge)
print(f"\n✅ {len(df_rge):,} lignes RGE téléchargées")

# ─── 2. NORMALISATION ─────────────────────────────────────────────────────────
df_rge[COL_SIRET] = df_rge[COL_SIRET].astype(str).str.zfill(14)
df_rge["siren"]   = df_rge[COL_SIRET].str[:9]

# ─── 3. SETS QUALIBAT ─────────────────────────────────────────────────────────
mask_qualibat  = df_rge[COL_ORG].str.contains("Qualibat", case=False, na=False)
siren_qualibat = set(df_rge[mask_qualibat]["siren"])
siret_qualibat = set(df_rge[mask_qualibat][COL_SIRET])
siren_rge      = set(df_rge["siren"])
siret_rge      = set(df_rge[COL_SIRET])

# Organismes présents (pour info)
print(f"\n📋 Organismes dans le fichier RGE :")
print(df_rge[COL_ORG].value_counts().to_string())

print(f"\n📊 Stats RGE :")
print(f"   SIREN Qualibat      : {len(siren_qualibat):,}")
print(f"   SIREN RGE total     : {len(siren_rge):,}")

# ─── 4. AGRÉGATION PAR SIRET (1 ligne par entreprise) ────────────────────────
df_rge_agg = df_rge.groupby(COL_SIRET).agg(
    siren                = ("siren",      "first"),
    rge_nom_entreprise   = (COL_NOM,      "first"),
    rge_organisme        = (COL_ORG,      lambda x: ", ".join(sorted(set(x.dropna())))),
    rge_qualifications   = (COL_QUALIF,   lambda x: "; ".join(sorted(set(x.dropna())))),
    rge_certificats      = (COL_CERT,     lambda x: ", ".join(sorted(set(x.dropna())))),
    rge_domaines         = (COL_DOMAINE,  lambda x: ", ".join(sorted(set(x.dropna())))),
    rge_date_fin         = (COL_DATE,     "max"),
    rge_email            = (COL_EMAIL,    "first"),   # ← bonus : email depuis RGE !
    rge_telephone        = (COL_TEL,      "first"),   # ← bonus : tel depuis RGE !
    rge_site_internet    = (COL_SITE,     "first"),   # ← bonus : site web !
    nb_qualifications_rge= (COL_SIRET,    "count"),
).reset_index().rename(columns={COL_SIRET: "siret"})

print(f"✅ {len(df_rge_agg):,} entreprises RGE uniques après agrégation")

# ─── 5. CHARGEMENT BASE KTIPIO ────────────────────────────────────────────────
print("\n📦 Chargement de votre base BTP...")
df_ktipio = pd.read_csv("btp_companies_filtered.csv", dtype=str)
df_ktipio["siren"] = df_ktipio["siren"].astype(str).str.zfill(9)
df_ktipio["siret"] = df_ktipio["siret"].astype(str).str.zfill(14)
print(f"   Entreprises dans votre base : {len(df_ktipio):,}")

# ─── 6. MERGE ─────────────────────────────────────────────────────────────────
df_ktipio = df_ktipio.merge(df_rge_agg[[
    "siret", "rge_organisme", "rge_qualifications", "rge_certificats",
    "rge_domaines", "rge_date_fin", "rge_email", "rge_telephone",
    "rge_site_internet", "nb_qualifications_rge"
]], on="siret", how="left")

df_ktipio["is_rge"]      = df_ktipio["nb_qualifications_rge"].fillna(0).astype(int) > 0
df_ktipio["is_qualibat"] = (
    df_ktipio["siret"].isin(siret_qualibat) |
    df_ktipio["siren"].isin(siren_qualibat)
)
df_ktipio["nb_qualifications_rge"] = df_ktipio["nb_qualifications_rge"].fillna(0).astype(int)
df_ktipio["is_federation"] = False

# ─── 7. STATS & EXPORT ────────────────────────────────────────────────────────
total       = len(df_ktipio)
nb_qualibat = df_ktipio["is_qualibat"].sum()
nb_rge      = df_ktipio["is_rge"].sum()
nb_email    = df_ktipio["rge_email"].notna().sum()
nb_tel      = df_ktipio["rge_telephone"].notna().sum()

print(f"\n📊 Résultats :")
print(f"   Total entreprises      : {total:,}")
print(f"   ✅ is_qualibat         : {nb_qualibat:,}  ({nb_qualibat/total*100:.1f}%)")
print(f"   ✅ is_rge              : {nb_rge:,}  ({nb_rge/total*100:.1f}%)")
print(f"   📧 emails récupérés   : {nb_email:,}")
print(f"   📞 téléphones récupérés: {nb_tel:,}")
print(f"   ⏳ is_federation       : en attente API FFB")

cols_export = [
    "siren", "siret",
    "is_qualibat", "is_rge", "is_federation",
    "nb_qualifications_rge",
    "rge_organisme", "rge_qualifications", "rge_certificats",
    "rge_domaines", "rge_date_fin",
    "rge_email", "rge_telephone", "rge_site_internet",
]
df_ktipio[cols_export].to_csv("enrichissement_qualibat_rge_detaille.csv", index=False)
print(f"\n✅ Fichier exporté : enrichissement_qualibat_rge_detaille.csv")