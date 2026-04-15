import pandas as pd

input_file = "StockEtablissement_utf8.parquet"
output_file = "btp2_uniquement.parquet"

print("🚀 Lecture du fichier StockEtablissement...")

cols = [
    'siren', 'siret',
    'statutDiffusionEtablissement',
    'etablissementSiege',
    'denominationUsuelleEtablissement',
    'enseigne1Etablissement', 'enseigne2Etablissement', 'enseigne3Etablissement',
    'numeroVoieEtablissement', 'typeVoieEtablissement', 'libelleVoieEtablissement',
    'codePostalEtablissement', 'libelleCommuneEtablissement', 'codeCommuneEtablissement',
    'activitePrincipaleEtablissement',
    'activitePrincipaleRegistreMetiersEtablissement',
    'etatAdministratifEtablissement',
    'caractereEmployeurEtablissement',
    'trancheEffectifsEtablissement',
    'dateCreationEtablissement',
]

df = pd.read_parquet(input_file, columns=cols)
print(f"📦 Total établissements chargés : {len(df):,}")

# ── Codes NAF BTP élargis ──────────────────────────────────────────────────
BTP_PREFIXES = (
    '41', '42', '43',                          # Cœur construction
    '7111', '7112', '7120',                    # Architecture, ingénierie, essais
    '4673',                                    # Négoce matériaux
    '7732',                                    # Location matériel BTP
    '3311', '3312', '3313',                    # Réparation équipements
    '2361', '2362', '2363', '2364', '2369',    # Béton, briques, tuiles
    '2331', '2332', '2370',                    # Carrelage, pierre
    '8110', '8121', '8122',                    # Facilities, nettoyage chantier
    '8130',                                    # Aménagement paysager
    '4311', '4312',                            # Démolition, forage
)

# ── Normalisation NAF (retire les points : "41.20" → "4120") ──────────────
naf = df['activitePrincipaleEtablissement'].fillna('').str.replace('.', '', regex=False)
naf_rm = df['activitePrincipaleRegistreMetiersEtablissement'].fillna('').str.replace('.', '', regex=False)

# ── Filtres ────────────────────────────────────────────────────────────────
mask_btp        = naf.str.startswith(BTP_PREFIXES) | naf_rm.str.startswith(BTP_PREFIXES)
mask_actif      = df['etatAdministratifEtablissement'] == 'A'
mask_diffusable = df['statutDiffusionEtablissement'].isin(['O', 'P'])
# ✅ CORRECTION PRINCIPALE : plus de filtre sur établissementSiege
# On garde sièges ET établissements secondaires

df_btp = df[mask_btp & mask_actif & mask_diffusable].copy()

# ── Stats utiles ───────────────────────────────────────────────────────────
nb_sieges      = df_btp['etablissementSiege'].sum()
nb_secondaires = (~df_btp['etablissementSiege']).sum()
print(f"✅ Total BTP actifs         : {len(df_btp):,}")
print(f"   ├─ Sièges sociaux        : {nb_sieges:,}")
print(f"   └─ Établissements second.: {nb_secondaires:,}")

# ── Nettoyage des noms ─────────────────────────────────────────────────────
def clean_name(row):
    for champ in ['denominationUsuelleEtablissement', 'enseigne1Etablissement',
                  'enseigne2Etablissement', 'enseigne3Etablissement']:
        val = row.get(champ)
        if pd.notnull(val) and str(val).strip():
            return str(val).strip().title()
    return f"Entreprise BTP ({row['siret']})"

print("🏷️  Nettoyage des noms...")
df_btp['name'] = df_btp.apply(clean_name, axis=1)

# ── Adresse reconstituée ───────────────────────────────────────────────────
df_btp['adresse'] = (
    df_btp['numeroVoieEtablissement'].fillna('').astype(str).str.strip() + ' ' +
    df_btp['typeVoieEtablissement'].fillna('').astype(str).str.strip() + ' ' +
    df_btp['libelleVoieEtablissement'].fillna('').astype(str).str.strip()
).str.strip().str.replace(r'\s+', ' ', regex=True)

# ── Sélection finale ───────────────────────────────────────────────────────
final_df = df_btp[[
    'siren', 'siret', 'name', 'adresse',
    'codePostalEtablissement', 'libelleCommuneEtablissement', 'codeCommuneEtablissement',
    'activitePrincipaleEtablissement', 'etablissementSiege',
    'trancheEffectifsEtablissement', 'caractereEmployeurEtablissement', 'dateCreationEtablissement',
]].rename(columns={
    'codePostalEtablissement':     'zip_code',
    'libelleCommuneEtablissement': 'city',
    'codeCommuneEtablissement':    'insee_commune',
    'activitePrincipaleEtablissement': 'naf_code',
    'etablissementSiege':          'is_siege',
    'trancheEffectifsEtablissement': 'tranche_effectifs',
    'caractereEmployeurEtablissement': 'employeur',
    'dateCreationEtablissement':   'date_creation',
})

final_df.to_parquet(output_file, index=False)
print(f"\n📊 Fichier sauvegardé : {output_file}")