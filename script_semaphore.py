import time
import pandas as pd
import threading
import queue
from pathlib import Path

def clean_name(name):
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in name)[:100]

def worker_thread_semaphore(task_queue, output_dir, semaphore):
    while True:
        with semaphore:
            try:
                item = task_queue.get(timeout=1)
                if item is None:
                    break
                   
                index, row = item
                category = str(row.get("Category", row.get("skills", row.get("Titre", "Inconnu")))).strip()
                clean_category = clean_name(category)
               
                output_path = Path(output_dir) / clean_category
                output_path.mkdir(exist_ok=True)
               
                with open(output_path / f"cv_{index}.txt", 'w', encoding='utf-8') as f:
                    f.write(f"=== CV {index} ===\n")
                    for col, val in row.items():
                        f.write(f"{col}: {val}\n")
                       
            except queue.Empty:
                continue

if __name__ == "__main__":
    start_time = time.time()
   
    input_file = "cvs (2).xlsx"
    output_folder = "CVs_Semaphore"
    num_threads = 4
    max_concurrent = 2  # Nombre max de threads simultanés
   
    print("Lecture du fichier Excel...")
    df = pd.read_excel(input_file, engine='openpyxl')
    total_cvs = len(df)
    print(f"{total_cvs} CVs à traiter avec {num_threads} threads (max {max_concurrent} simultanés)")
   
    # File et sémaphore
    task_queue = queue.Queue()
    semaphore = threading.Semaphore(max_concurrent)
   
    # Lancement des threads
    print("Lancement des threads avec sémaphore...")
    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=worker_thread_semaphore,
            args=(task_queue, output_folder, semaphore)
        )
        t.start()
        threads.append(t)
   
    # Envoi des tâches
    print("Distribution des tâches...")
    for index, row in df.iterrows():
        task_queue.put((index, row))
   
    # Signal de fin
    for _ in range(num_threads):
        task_queue.put(None)
   
    # Attente fin des threads
    for t in threads:
        t.join()
   
    elapsed = time.time() - start_time
    print(f"\nTemps d'exécution avec sémaphore: {elapsed:.2f} secondes")
    print(f"CVs traités: {total_cvs}")
    print(f"Taux: {total_cvs/elapsed:.2f} CVs/seconde")