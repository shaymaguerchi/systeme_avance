import multiprocessing
import time
import pandas as pd
import threading
import queue
import os
from pathlib import Path

def clean_name(name):
    """Nettoie les noms de fichiers pour Windows/Linux"""
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in str(name))[:100]

def calculate_thread_counts():
    """Calcule dynamiquement le nombre de threads et la concurrence max"""
    cpu_count = os.cpu_count() or 1
    max_threads = min(cpu_count * 2, 8)  # Maximum 8 threads
    max_concurrent = max(1, cpu_count // 2)  # 50% des coeurs pour la concurrence
    return max_threads, max_concurrent

def worker_thread_semaphore(task_queue, output_dir, semaphore, result_counter):
    """Thread worker avec contrôle de concurrence par sémaphore"""
    while True:
        try:
            item = task_queue.get(timeout=1)
            if item is None:
                break

            index, row = item
            with semaphore:  # Contrôle d'accès concurrent ici
                try:
                    # Détection de catégorie robuste
                    category = next(
                        (str(row[col]).strip() 
                        for col in ['Category', 'skills', 'Titre'] 
                        if col in row and pd.notna(row[col])),
                        "Inconnu"
                    )
                    
                    clean_category = clean_name(category)
                    output_path = Path(output_dir) / clean_category
                    
                    # Création thread-safe du dossier
                    output_path.mkdir(exist_ok=True)
                    
                    # Écriture atomique avec fichier temporaire
                    temp_file = output_path / f"cv_{index}.tmp"
                    final_file = output_path / f"cv_{index}.txt"
                    
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        f.write(f"=== CV {index} ===\n")
                        for col, val in row.items():
                            f.write(f"{col}: {val}\n")
                    
                    temp_file.rename(final_file)
                    
                    with result_counter.get_lock():
                        result_counter.value += 1
                        
                    print(f"Traited {index+1}/{total_cvs} (Active threads: {threading.active_count()-1})", end='\r')
                
                except Exception as e:
                    print(f"\nError processing CV {index}: {str(e)}")

        except queue.Empty:
            continue

if __name__ == "__main__":
    start_time = time.time()
    
    # Configuration dynamique
    input_file = "cvs (2).xlsx"
    output_folder = "CVs_Semaphore_Optimized"
    num_threads, max_concurrent = calculate_thread_counts()
    
    print(f"\n⚙️ Configuration:")
    print(f"- Threads totaux: {num_threads}")
    print(f"- Concurrence max: {max_concurrent}")
    print(f"- Dossier sortie: {output_folder}")

    # Lecture des données
    print("\n📖 Lecture du fichier Excel...")
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        total_cvs = len(df)
        print(f"🔢 Total CVs à traiter: {total_cvs}")
    except Exception as e:
        print(f"❌ Erreur lecture fichier: {str(e)}")
        exit(1)

    # Initialisation
    task_queue = queue.Queue(maxsize=num_threads*2)
    semaphore = threading.Semaphore(max_concurrent)
    result_counter = multiprocessing.Value('i', 0)
    
    # Dossier de sortie
    Path(output_folder).mkdir(exist_ok=True)

    # Lancement des threads
    print("\n🚀 Lancement des threads...")
    threads = []
    for i in range(num_threads):
        t = threading.Thread(
            target=worker_thread_semaphore,
            args=(task_queue, output_folder, semaphore, result_counter),
            name=f"Worker-{i+1}",
            daemon=True
        )
        t.start()
        threads.append(t)

    # Envoi des tâches avec progression
    print("\n📤 Distribution des tâches...")
    for index, row in df.iterrows():
        task_queue.put((index, row))
        if (index + 1) % 10 == 0:
            print(f"Enqueued {index+1}/{total_cvs}", end='\r')

    # Signal de fin
    for _ in range(num_threads):
        task_queue.put(None)

    # Attente avec timeout
    print("\n\n🕒 Attente de la fin des threads...")
    timeout = 30  # secondes
    start_wait = time.time()
    
    for t in threads:
        t.join(timeout=max(0, timeout - (time.time() - start_wait)))
        if t.is_alive():
            print(f"⚠️ Thread {t.name} n'a pas terminé dans le temps imparti")

    # Résultats
    elapsed = time.time() - start_time
    processed = result_counter.value
    success_rate = (processed / total_cvs) * 100

    print(f"\n✅ Traitement terminé en {elapsed:.2f}s")
    print(f"📊 CVs traités: {processed}/{total_cvs} ({success_rate:.1f}%)")
    print(f"⚡ Performance: {processed/elapsed:.2f} CVs/s")
    print(f"📁 Dossier sortie: {Path(output_folder).absolute()}")