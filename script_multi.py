# multiprocess_version.py
import time
import pandas as pd
import multiprocessing
from pathlib import Path

def clean_name(name):
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in name)[:100]

def worker_process(queue, output_dir):
    while True:
        item = queue.get()
        if item is None:
            break
       
        index, row = item
        # Détection catégorie (simplifiée pour l'exemple)
        category = str(row.get("Category", row.get("skills", row.get("Titre", "Inconnu")))).strip()
        clean_category = clean_name(category)
       
        # Création dossier et fichier
        output_path = Path(output_dir) / clean_category
        output_path.mkdir(exist_ok=True)
       
        with open(output_path / f"cv_{index}.txt", 'w', encoding='utf-8') as f:
            f.write(f"=== CV {index} ===\n")
            for col, val in row.items():
                f.write(f"{col}: {val}\n")

if __name__ == "__main__":
    start_time = time.time()
   
    input_file = "cvs (2).xlsx"
    output_folder = "CVs_Multiprocess"
    num_processes = 4
   
    print("Lecture du fichier Excel...")
    df = pd.read_excel(input_file, engine='openpyxl')
    total_cvs = len(df)
    print(f"{total_cvs} CVs à traiter avec {num_processes} processus")
   
    # Configuration pour Windows
    multiprocessing.set_start_method('spawn')
   
    # File de travail
    task_queue = multiprocessing.Queue()
   
    # Lancement des workers
    print("Lancement des processus...")
    processes = []
    for _ in range(num_processes):
        p = multiprocessing.Process(target=worker_process, args=(task_queue, output_folder))
        p.start()
        processes.append(p)
   
    # Envoi des tâches
    print("Distribution des tâches...")
    for index, row in df.iterrows():
        task_queue.put((index, row))
   
    # Signal de fin
    for _ in range(num_processes):
        task_queue.put(None)
   
    # Attente fin des processus
    for p in processes:
        p.join()
   
    elapsed = time.time() - start_time
    print(f"\nTemps d'exécution multiprocess: {elapsed:.2f} secondes")
    print(f"CVs traités: {total_cvs}")
    print(f"Taux: {total_cvs/elapsed:.2f} CVs/seconde")