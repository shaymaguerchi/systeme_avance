import time
import pandas as pd
import multiprocessing
import os
from pathlib import Path

def clean_name(name):
    """Nettoie les noms pour les systèmes de fichiers"""
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in name)[:100]

def worker_process(queue, output_dir):
    """Fonction exécutée par chaque processus worker"""
    while True:
        try:
            item = queue.get()
            if item is None:  # Signal de fin
                break
                
            index, row = item
            # Détection dynamique de la catégorie
            category = str(row.get("Category", row.get("skills", row.get("Titre", "Inconnu")))).strip()
            clean_category = clean_name(category)
            
            # Création du dossier (chaque processus a son propre espace)
            output_path = Path(output_dir) / clean_category
            output_path.mkdir(exist_ok=True)
            
            # Écriture du fichier
            with open(output_path / f"cv_{index}.txt", 'w', encoding='utf-8') as f:
                f.write(f"=== CV {index} ===\n")
                for col, val in row.items():
                    f.write(f"{col}: {val}\n")
                    
        except Exception as e:
            print(f"Erreur dans le worker: {e}")

def calculate_optimal_processes():
    """Calcule le nombre optimal de processus"""
    cpu_count = os.cpu_count() or 1
    # On prend 75% des coeurs disponibles pour laisser de la marge
    return max(1, int(cpu_count * 0.75))

if __name__ == "__main__":
    start_time = time.time()
    
    # Configuration
    input_file = "cvs (2).xlsx"
    output_folder = "CVs_Multiprocess_Optimized"
    num_processes = calculate_optimal_processes()
    
    print("\n=== Début du traitement multiprocess ===")
    print(f"Utilisation de {num_processes} processus (sur {os.cpu_count()} coeurs disponibles)")
    
    # Lecture des données
    print("Lecture du fichier Excel...")
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        total_cvs = len(df)
        print(f"{total_cvs} CVs à traiter")
    except Exception as e:
        print(f"Erreur de lecture du fichier: {e}")
        exit(1)

    # Configuration multiprocessing
    multiprocessing.set_start_method('spawn')  # Obligatoire pour Windows
    task_queue = multiprocessing.Queue(maxsize=num_processes*2)  # Taille tampon optimisée
    
    # Lancement des workers
    print("Lancement des processus workers...")
    processes = []
    for i in range(num_processes):
        p = multiprocessing.Process(
            target=worker_process,
            args=(task_queue, output_folder),
            name=f"Worker-{i+1}"
        )
        p.start()
        processes.append(p)
    
    # Distribution des tâches avec suivi
    print("Distribution des tâches...")
    processed_count = 0
    for index, row in df.iterrows():
        task_queue.put((index, row))
        processed_count += 1
        if processed_count % 10 == 0:
            print(f"Soumis: {processed_count}/{total_cvs}", end='\r')
    
    # Signal de fin pour chaque worker
    for _ in range(num_processes):
        task_queue.put(None)
    
    # Attente avec timeout et gestion des erreurs
    print("\nAttente de la fin des processus...")
    for p in processes:
        p.join(timeout=30)  # Timeout de sécurité
        if p.is_alive():
            print(f"Attention: le processus {p.name} n'a pas terminé dans le temps imparti")
    
    # Résultats
    elapsed = time.time() - start_time
    print("\n=== Résultats ===")
    print(f"Temps total: {elapsed:.2f} secondes")
    print(f"CVs traités: {total_cvs}")
    print(f"Performance: {total_cvs/elapsed:.2f} CVs/seconde")
    print(f"Dossier de sortie: {Path(output_folder).resolve()}")