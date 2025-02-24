import tkinter as tk
from tkinter import filedialog
import json
import os

def create_config():
    # Ordner auswählen
    root = tk.Tk()
    root.withdraw()  # Versteckt das Hauptfenster
    folder_path = filedialog.askdirectory(title="Ordner, in den die Konfigurationsdatei gespeichert werden soll")
    
    if not folder_path:
        print("No folder selected. Exiting.")
        return

    # Leere Konfigurationsdatei erstellen
    config_data = {
        "database": {
            "host": "",
            "port": "",
            "service-name": "",
            "username": "",
            "password": ""
        },
        "input_directory": "",
        "batch_size": ""
    }

    config_file_path = os.path.join(folder_path, "config.json")
    
    with open(config_file_path, 'w') as config_file:
        json.dump(config_data, config_file, indent=4)
    
    print(f"Leere Konfigurationsdatei wurde erstellt: {config_file_path}")