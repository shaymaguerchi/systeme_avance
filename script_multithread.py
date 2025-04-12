import time
import pandas as pd
import threading
import queue
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

def clean_name(name):
    """Nettoie les noms de fichiers pour Windows/Linux"""
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in name)[:100]

def process_item(index, row, output_dir, fs_lock):
    """Traite un élément individuel de CV"""
    try:
        # Détection de la catégorie avec fallback
        category = str(row.get("Category", row.get("skills", row.get("Titre", "Inconnu")))).strip()
        clean_category = clean_name(category)
        
        # Création thread-safe du dossier
        output_path = Path(output_dir) / clean_category
        
        with fs_lock:
            output_path.mkdir(exist_ok=True)
        
        # Écriture du fichier
        with open(output_path / f"cv_{index}.txt", 'w', encoding='utf-8') as f:
            f.write(f"=== CV {index} ===\n")
            for col, val in row.items():
                f.write(f"{col}: {val}\n")
                
        return True
    except Exception as e:
        print(f"Erreur traitement CV {index}: {str(e)}")
        return False

def main():
    start_time = time.time()
    
    # Configuration
    input_file = "cvs (2).xlsx"
    output_folder = "CVs_Multithread_Optimized"
    num_threads = min(8, (os.cpu_count() or 1) * 2)  # Adaptatif avec limite haute
    
    print("\n=== Début du traitement ===")
    print(f"Configuration: {num_threads} threads")
    
    # Lecture des données
    print("Lecture du fichier Excel...")
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        total_cvs = len(df)
        print(f"{total_cvs} CVs à traiter")
    except Exception as e:
        print(f"Erreur lecture fichier: {str(e)}")
        return

    # Préparation de l'environnement
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True)
    
    # Verrou pour opérations filesystem
    fs_lock = threading.Lock()
    
    # Compteur de tâches réussies
    processed_count = 0
    
    # Traitement avec ThreadPoolExecutor (meilleure gestion que les threads manuels)
    print("Lancement du traitement multithread...")
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        
        # Soumission des tâches
        for index, row in df.iterrows():
            futures.append(
                executor.submit(
                    process_item,
                    index=index,
                    row=row,
                    output_dir=output_folder,
                    fs_lock=fs_lock
                )
            )
        
        # Surveillance de la progression
        for i, future in enumerate(futures, 1):
            try:
                if future.result():  # Bloque jusqu'à complétion
                    processed_count += 1
                
                if i % 10 == 0 or i == total_cvs:
                    print(f"Progression: {i}/{total_cvs} ({i/total_cvs:.1%})")
            except Exception as e:
                print(f"Erreur dans le futur {i}: {str(e)}")
    
    # Statistiques
    elapsed = time.time() - start_time
    success_rate = processed_count / total_cvs
    
    print("\n=== Résultats ===")
    print(f"Temps total: {elapsed:.2f}s")
    print(f"CVs traités: {processed_count}/{total_cvs} ({success_rate:.1%})")
    print(f"Performance: {total_cvs/elapsed:.2f} CVs/s")
    print(f"Dossier sortie: {output_path.resolve()}")

if __name__ == "__main__":
    main()