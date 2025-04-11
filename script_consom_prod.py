import time
import pandas as pd
import threading
import queue
from pathlib import Path

def clean_name(name):
    """Nettoie les noms de dossiers pour Windows"""
    invalid_chars = r'<>:"/\|?*'
    return ''.join(c if c not in invalid_chars else '_' for c in str(name))[:100]

def producer(task_queue, condition, df):
    """Producteur : alimente la queue avec les CVs"""
    for index, row in df.iterrows():
        with condition:
            # Ajout d'un CV dans la queue
            task_queue.put((index, row))
            condition.notify()  # Réveille un consommateur
            print(f"→ Producteur a ajouté CV {index}", end='\r')
   
    # Signal de fin
    with condition:
        for _ in range(consumers_count):  # Un signal par consommateur
            task_queue.put(None)
            condition.notify()

def consumer(task_queue, result_queue, condition, output_dir):
    """Consommateur : traite les CVs de la queue"""
    while True:
        with condition:
            while task_queue.empty():
                condition.wait()  # Attend des tâches
            item = task_queue.get()
            if item is None:  # Signal de fin
                break
           
            index, row = item
            try:
                # Détection de la catégorie
                category = "Inconnu"
                for col in ['Category', 'skills', 'Titre']:
                    if col in row and pd.notna(row[col]):
                        category = str(row[col]).strip()
                        break
               
                # Création du dossier
                clean_cat = clean_name(category)
                category_dir = Path(output_dir) / clean_cat
                category_dir.mkdir(exist_ok=True)
               
                # Écriture du fichier
                with open(category_dir / f"cv_{index}.txt", 'w', encoding='utf-8') as f:
                    f.write(f"=== CV {index} ===\n")
                    for col, val in row.items():
                        f.write(f"{col}: {val}\n")
               
                result_queue.put(1)  # Comptabilisation
                print(f"← Consommateur a traité CV {index}", end='\r')
           
            except Exception as e:
                print(f"Erreur CV {index}: {str(e)}")

if __name__ == "__main__":
    # Configuration
    input_file = "cvs (2).xlsx"          # Chemin vers le fichier Excel
    output_folder = "CVs_ProdCons"    # Dossier de sortie
    producers_count = 1               # 1 producteur
    consumers_count = 4               # 4 consommateurs
   
    start_time = time.time()
   
    # Initialisation
    task_queue = queue.Queue(maxsize=10)  # Taille limitée pour éviter la surcharge
    result_queue = queue.Queue()
    condition = threading.Condition()
   
    # Lecture du fichier
    print("Lecture du fichier Excel...")
    df = pd.read_excel(input_file, engine='openpyxl')
    total_cvs = len(df)
    print(f"{total_cvs} CVs à traiter avec {producers_count}p/{consumers_count}c")
   
    # Lancement des consommateurs
    consumers = []
    for i in range(consumers_count):
        t = threading.Thread(
            target=consumer,
            args=(task_queue, result_queue, condition, output_folder),
            name=f"Consommateur-{i}"
        )
        t.start()
        consumers.append(t)
   
    # Lancement du producteur
    producer_thread = threading.Thread(
        target=producer,
        args=(task_queue, condition, df),
        name="Producteur"
    )
    producer_thread.start()
   
    # Attente
    producer_thread.join()
    for t in consumers:
        t.join()
   
    # Résultats
    elapsed = time.time() - start_time
    processed = result_queue.qsize()
   
    print(f"\n✅ Traitement terminé en {elapsed:.2f}s")
    print(f"CVs traités : {processed}/{total_cvs}")
    print(f"Taux : {processed/elapsed:.2f} CVs/s")
    print(f"Dossier de sortie : {output_folder}")

    input_excel = "cvs (2).xlsx"  # Remplacez par votre fichier
    output_folder = "CVs_ProdCons"

    