import pandas as pd

# 1. Configuration
input_file = "StockEtablissement_utf8.parquet"
output_file = "btp_uniquement.parquet"

print("🚀 Lecture du fichier StockEtablissement...")

# 2. Colonnes utiles (ajout siren, adresse complète, statut diffusion)
cols = [
    'siren',
    'siret',
    'statutDiffusionEtablissement',       # 'O' ou 'P' = diffusable
    'etablissementSiege',                  # True = siège social
    'denominationUsuelleEtablissement',
    'enseigne1Etablissement',
    'enseigne2Etablissement',
    'enseigne3Etablissement',
    'numeroVoieEtablissement',
    'typeVoieEtablissement',
    'libelleVoieEtablissement',
    'codePostalEtablissement',
    'libelleCommuneEtablissement',
    'codeCommuneEtablissement',
    'activitePrincipaleEtablissement',
    'activitePrincipaleRegistreMetiersEtablissement',
    'etatAdministratifEtablissement',      # 'A' = Actif
    'caractereEmployeurEtablissement',
    'trancheEffectifsEtablissement',
    'dateCreationEtablissement',
]

# 3. Chargement
df = pd.read_parquet(input_file, columns=cols)
print(f"📦 Total établissements chargés : {len(df):,}")

# 4. Codes NAF BTP élargis (pas seulement 41/42/43 !)
BTP_PREFIXES = (
    # Cœur BTP
    '41', '42', '43',
    # Architecture & ingénierie BTP
    '7111', '7112',
    # Essais et analyses techniques (contrôle bâtiment)
    '7120',
    # Commerce de gros matériaux de construction
    '4673',
    # Location matériel construction & démolition
    '7732',
    # Réparation équipements
    '3311', '3312',
    # Fabrication matériaux (béton, briques, tuiles)
    '2361', '2362', '2363', '2364',
    # Facilities / maintenance immobilière
    '8110',
)

# 5. Filtrage : BTP actif + diffusable
print("🔍 Filtrage BTP élargi, actifs, diffusables...")

naf = df['activitePrincipaleEtablissement'].fillna('').str.replace('.', '', regex=False)
naf_rm = df['activitePrincipaleRegistreMetiersEtablissement'].fillna('').str.replace('.', '', regex=False)

mask_btp = (
    naf.str.startswith(BTP_PREFIXES) |
    naf_rm.str.startswith(BTP_PREFIXES)
)
mask_actif = df['etatAdministratifEtablissement'] == 'A'
mask_diffusable = df['statutDiffusionEtablissement'].isin(['O', 'P'])  # ← correction : 'P' exclu avant

df_btp = df[mask_btp & mask_actif & mask_diffusable].copy()
print(f"✅ Établissements BTP actifs trouvés : {len(df_btp):,}")

# 6. Nettoyage du nom (cherche dans 5 champs dans l'ordre)
def clean_name(row):
    for champ in [
        'denominationUsuelleEtablissement',
        'enseigne1Etablissement',
        'enseigne2Etablissement',
        'enseigne3Etablissement',
    ]:
        val = row.get(champ)
        if pd.notnull(val) and str(val).strip():
            return str(val).strip().title()
    return f"Entreprise BTP ({row['siret']})"

print("🏷️  Nettoyage des noms...")
df_btp['name'] = df_btp.apply(clean_name, axis=1)

# 7. Adresse complète reconstituée
df_btp['adresse'] = (
    df_btp['numeroVoieEtablissement'].fillna('').astype(str).str.strip() + ' ' +
    df_btp['typeVoieEtablissement'].fillna('').astype(str).str.strip() + ' ' +
    df_btp['libelleVoieEtablissement'].fillna('').astype(str).str.strip()
).str.strip().str.replace(r'\s+', ' ', regex=True)

# 8. Sélection et renommage final
final_df = df_btp[[
    'siren',
    'siret',
    'name',
    'adresse',
    'codePostalEtablissement',
    'libelleCommuneEtablissement',
    'codeCommuneEtablissement',
    'activitePrincipaleEtablissement',
    'etablissementSiege',
    'trancheEffectifsEtablissement',
    'caractereEmployeurEtablissement',
    'dateCreationEtablissement',
]].rename(columns={
    'codePostalEtablissement':    'zip_code',
    'libelleCommuneEtablissement':'city',
    'codeCommuneEtablissement':   'insee_commune',
    'activitePrincipaleEtablissement': 'naf_code',
    'etablissementSiege':         'is_siege',
    'trancheEffectifsEtablissement': 'tranche_effectifs',
    'caractereEmployeurEtablissement': 'employeur',
    'dateCreationEtablissement':  'date_creation',
})

# 9. Sauvegarde
final_df.to_parquet(output_file, index=False)

print(f"\n📊 Résumé :")
print(f"   • Établissements BTP actifs : {len(final_df):,}")
print(f"   • Dont sièges sociaux       : {final_df['is_siege'].sum():,}")
print(f"   • Fichier sauvegardé        : {output_file}")