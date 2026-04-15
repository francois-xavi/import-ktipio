import pandas as pd
import os

file_path = r'c:\Users\Dell\Documents\projects\pipeline_projects\siren\enrichissement_qualibat_rge.csv'

if not os.path.exists(file_path):
    print(f"❌ Erreur : Le fichier {file_path} n'existe pas.")
else:
    # On lit uniquement les colonnes nécessaires pour aller plus vite
    cols = ['is_qualibat', 'is_rge', 'is_federation']
    df = pd.read_csv(file_path, usecols=cols)
    total = len(df)
    
    with open('stats_results.txt', 'w', encoding='utf-8') as f:
        f.write(f"📊 Statistiques de {os.path.basename(file_path)}\n")
        f.write(f"{'─' * 40}\n")
        f.write(f"Total entreprises : {total:,}\n")
        f.write(f"{'─' * 40}\n")
        
        for col in cols:
            if col in df.columns:
                # Convertir en bool (important car le CSV peut être hétérogène)
                # On map les valeurs courantes vers des booleens
                count = (df[col].astype(str).str.lower().isin(['true', '1'])).sum()
                percent = (count / total * 100) if total > 0 else 0
                f.write(f"✅ {col:<15} : {int(count):>9,}  ({percent:>6.2f}%)\n")
            else:
                f.write(f"⚠️ {col:<15} : Colonne absente\n")
        f.write(f"{'─' * 40}\n")
    print("Stats written to stats_results.txt")
