import pandas as pd

# 1. Configuration
input_file = "StockEtablissement_utf8.parquet" 
output_file = "btp_complet.parquet"

print("🚀 Analyse du fichier (2.0 Go)... Cette opération utilise de la mémoire RAM.")

try:
    # 2. On charge TOUTES les colonnes (on ne spécifie pas 'columns')
    # Note : Si votre PC a moins de 16Go de RAM, cela peut être lent.
    df = pd.read_parquet(input_file)
    
    print(f"✅ Fichier chargé. Nombre total de colonnes détectées : {len(df.columns)}")
    print("🔍 Filtrage des entreprises BTP (41, 42, 43)...")

    # 3. Filtrage BTP
    # On s'assure que la colonne est bien traitée comme du texte
    df['activitePrincipaleEtablissement'] = df['activitePrincipaleEtablissement'].astype(str)
    
    mask = df['activitePrincipaleEtablissement'].str.startswith(('41', '42', '43'), na=False)
    df_btp = df[mask].copy()

    # 4. Sauvegarde de TOUTES les colonnes pour les lignes BTP
    print(f"💾 Sauvegarde de {len(df_btp)} entreprises avec toutes leurs colonnes...")
    df_btp.to_parquet(output_file, index=False)

    print("-" * 30)
    print(f"✅ TERMINÉ !")
    print(f"📁 Fichier créé : {output_file}")
    print(f"💡 Vous pouvez maintenant uploader ce fichier sur Replit.")
    print("-" * 30)

except MemoryError:
    print("❌ Erreur : Pas assez de mémoire RAM pour charger toutes les colonnes d'un coup.")
    print("💡 Conseil : Utilisez le script précédent qui ne sélectionne que les colonnes nécessaires.")
except Exception as e:
    print(f"❌ Une erreur est survenue : {e}")