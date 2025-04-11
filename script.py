# sequential_version.py
import time
import pandas as pd
import os
from pathlib import Path

def clean_name(name):
    """Nettoie les noms pour Windows"""
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name[:100]

def process_cvs_sequential(input_file, output_dir):
    start_time = time.time()
   
    print("Lecture du fichier Excel...")
    df = pd.read_excel(input_file, engine='openpyxl')
    total_cvs = len(df)
    print(f"{total_cvs} CVs à traiter")
   
    # Création du dossier de sortie
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
   
    print("Début du traitement séquentiel...")
    for index, row in df.iterrows():
        # Détection de la catégorie
        if "Category" in df.columns and pd.notna(row["Category"]):
            category = str(row["Category"]).strip()
        elif "skills" in df.columns and pd.notna(row["skills"]):
            category = str(row["skills"]).strip()
        elif "Titre" in df.columns and pd.notna(row["Titre"]):
            category = str(row["Titre"]).strip()
        else:
            category = "Inconnu"
       
        # Nettoyage et création du dossier
        clean_category = clean_name(category)
        category_dir = output_path / clean_category
        category_dir.mkdir(exist_ok=True)
       
        # Écriture du fichier
        output_file = category_dir / f"cv_{index}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"=== CV {index} ===\n")
            for col in df.columns:
                f.write(f"{col}: {row[col]}\n")
       
        # Progression
        if (index + 1) % 10 == 0:
            print(f"Traités: {index + 1}/{total_cvs}")
   
    elapsed = time.time() - start_time
    print(f"\nTemps d'exécution séquentiel: {elapsed:.2f} secondes")
    print(f"CVs traités: {total_cvs}")
    print(f"Taux: {total_cvs/elapsed:.2f} CVs/seconde")

if __name__ == "__main__":
    input_excel = "cvs (2).xlsx"  # Remplacez par votre fichier
    output_folder = "CVs_Sequential"
   
    process_cvs_sequential(input_excel, output_folder)