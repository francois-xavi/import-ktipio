import pandas as pd
import json

# Nom de ton fichier source
input_file = "btp_complet.parquet"
output_json = "colonnes.json"

print(f"Reading metadata from {input_file}...")

try:
    # On lit uniquement la première ligne pour obtenir les colonnes sans charger le fichier entier
    df_sample = pd.read_parquet(input_file, engine='pyarrow').head(0)
    
    # Récupérer la liste des colonnes
    columns_list = df_sample.columns.tolist()
    
    # Créer un dictionnaire avec le nombre total et la liste
    data_info = {
        "total_columns": len(columns_list),
        "columns": columns_list
    }
    
    # Sauvegarder en JSON
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(data_info, f, indent=4, ensure_ascii=False)
        
    print(f"✅ Succès ! {len(columns_list)} colonnes trouvées.")
    print(f"📁 La liste a été sauvegardée dans : {output_json}")

except Exception as e:
    print(f"❌ Erreur lors de la lecture : {e}")