import time
import pandas as pd
import threading
import queue
import os
from pathlib import Path

def clean_name(name):
    """Nettoie les noms de dossiers pour Windows"""
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in str(name))[:100]

def calculate_optimal_consumers():
    """Calcule le nombre optimal de consommateurs"""
    cpu_count = os.cpu_count() or 1
    # Pour du I/O bound (écriture fichiers), on peut dépasser le nombre de coeurs
    return min(cpu_count * 2, 8)  # Maximum 8 threads

def producer(task_queue, condition, df):
    """Producteur : alimente la queue avec les CVs"""
    for index, row in df.iterrows():
        with condition:
            while task_queue.full():  # Attente si queue pleine
                condition.wait()
            task_queue.put((index, row))
            condition.notify_all()  # Réveille tous les consommateurs
            print(f"→ Produit CV {index+1}/{len(df)}", end='\r')
   
    # Signal de fin
    with condition:
        for _ in range(consumers_count):
            task_queue.put(None)
            condition.notify_all()

def consumer(task_queue, result_queue, condition, output_dir, lock):
    """Consommateur : traite les CVs de la queue"""
    while True:
        with condition:
            while task_queue.empty():
                condition.wait()
            item = task_queue.get()
            condition.notify()  # Réveille le producteur si besoin
            if item is None:
                break
           
            index, row = item
            try:
                # Détection de la catégorie plus robuste
                category = next(
                    (str(row[col]).strip() 
                    for col in ['Category', 'skills', 'Titre'] 
                    if col in row and pd.notna(row[col])),
                    "Inconnu"
                )
               
                # Opérations filesystem thread-safe
                clean_cat = clean_name(category)
                category_dir = Path(output_dir) / clean_cat
                
                with lock:
                    category_dir.mkdir(exist_ok=True)
                
                # Écriture atomique
                temp_file = category_dir / f"cv_{index}.tmp"
                final_file = category_dir / f"cv_{index}.txt"
                
                with open(temp_file, 'w', encoding='utf-8') as f:
                    f.write(f"=== CV {index} ===\n")
                    for col, val in row.items():
                        f.write(f"{col}: {val}\n")
                
                temp_file.rename(final_file)
               
                result_queue.put(1)
                print(f"← Traité CV {index+1}/{total_cvs}", end='\r')
           
            except Exception as e:
                print(f"\n⚠️ Erreur CV {index}: {str(e)}")

if __name__ == "__main__":
    # Configuration dynamique
    input_file = "cvs (2).xlsx"
    output_folder = "CVs_ProdCons_Optimized"
    producers_count = 1
    consumers_count = calculate_optimal_consumers()
    
    print(f"\n⚡ Configuration: {producers_count} producteur, {consumers_count} consommateurs")

    # Initialisation
    start_time = time.time()
    task_queue = queue.Queue(maxsize=consumers_count * 2)  # Taille basée sur les workers
    result_queue = queue.Queue()
    condition = threading.Condition()
    fs_lock = threading.Lock()

    # Lecture du fichier
    try:
        print("📖 Lecture du fichier Excel...")
        df = pd.read_excel(input_file, engine='openpyxl')
        total_cvs = len(df)
        print(f"🔢 {total_cvs} CVs à traiter")
    except Exception as e:
        print(f"❌ Erreur lecture fichier: {str(e)}")
        exit(1)

    # Dossier de sortie
    Path(output_folder).mkdir(exist_ok=True)

    # Lancement des consommateurs
    consumers = []
    print("🚀 Lancement des consommateurs...")
    for i in range(consumers_count):
        t = threading.Thread(
            target=consumer,
            args=(task_queue, result_queue, condition, output_folder, fs_lock),
            name=f"Consumer-{i+1}",
            daemon=True
        )
        t.start()
        consumers.append(t)

    # Lancement du producteur
    producer_thread = threading.Thread(
        target=producer,
        args=(task_queue, condition, df),
        name="Producer"
    )
    producer_thread.start()

    # Surveillance
    print("\n🔄 Traitement en cours...")
    producer_thread.join()
    
    # Attente active avec timeout
    timeout = 30  # secondes
    start_wait = time.time()
    while any(t.is_alive() for t in consumers):
        if time.time() - start_wait > timeout:
            print("\n⚠️ Timeout atteint, certains threads sont bloqués")
            break
        time.sleep(0.1)

    # Résultats
    elapsed = time.time() - start_time
    processed = result_queue.qsize()
    success_rate = (processed / total_cvs) * 100

    print(f"\n✅ Traitement terminé en {elapsed:.2f}s")
    print(f"📊 CVs traités: {processed}/{total_cvs} ({success_rate:.1f}%)")
    print(f"⚡ Performance: {processed/elapsed:.2f} CVs/s")
    print(f"📁 Dossier: {Path(output_folder).absolute()}")