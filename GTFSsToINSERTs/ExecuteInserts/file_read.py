from tkinter import filedialog
import tkinter as tk
import sys
import os


def get_folder(heading="Wählen Sie einen Ordner aus"):
    """
    Diese Funktion öffnet ein Fenster, in dem der Benutzer den Ordner auswählt, und übergibt
    den Pfad des ausgewählten Ordners an die Funktion create_csv_from_gtfs zur Verarbeitung.
    """
    # Erstelle das Tkinter-Hauptfenster, aber verstecke es, da wir nur den Dateidialog verwenden
    root = tk.Tk()
    root.withdraw()  # Das Fenster wird nicht angezeigt, nur der Dialog wird geöffnet

    # Öffne den Ordner-Auswahl-Dialog, der es dem Benutzer ermöglicht, einen Ordner auszuwählen
    folder_path = filedialog.askdirectory(title = heading)

    # Wenn der Benutzer keinen Ordner auswählt, bricht das Skript ab
    if not folder_path:
        print("Kein Ordner ausgewählt. Das Skript wird beendet.", file=sys.stderr)
        sys.exit(1) 

    # Gebe den Pfad des ausgewählten Ordners zurück
    return folder_path


def get_file_handle(file_path):
    """
    Diese Funktion öffnet eine Datei im Lesemodus und gibt den Dateihandle zurück.
    """
    try:
        file = open(file_path, "r")
        return file
    except Exception as e:
        print(f"Die Datei  Datei {file_path}: {e}", file=sys.stderr)
        sys.exit(1)

def get_txt_files_in_path(folder_path):
    """
    Diese Funktion listet alle .txt-Dateien im angegebenen Ordner auf und gibt sie in einer Map zurück,
    wobei die Dateiendung .txt vom Key entfernt wird.
    """
    try:
        # Alle Dateien im Ordner auflisten, die mit .txt enden
        txt_files = [f for f in os.listdir(folder_path) if f.endswith(".txt")]

        file_map = {}

        for txt_file in txt_files:
            # Entferne die .txt-Erweiterung und speichere den Pfad in der Map
            filename_without_extension = os.path.splitext(txt_file)[0]
            file_map[filename_without_extension] = os.path.join(folder_path, txt_file)

        return file_map

    except Exception as e:
        print(f"Fehler beim Auflisten der .txt-Dateien im Ordner {folder_path}: {e}", file=sys.stderr)
        sys.exit(1)


def clean_and_split_line(line):
    """
    Diese Funktion entfernt führende und nachfolgende Leerzeichen, entfernt Leerzeichen zwischen
    den Werten, entfernt Anführungszeichen um die Werte und teilt die Zeile anhand von Kommas
    """
    # Entferne Anführungszeichen (") und trimme Leerzeichen
    line = line.strip().replace('"', '') 
    
    # Teile die Zeile anhand von Kommas, entferne zusätzlich Leerzeichen zwischen den Kommas und Werten
    values = [col.strip() for col in line.split(",")]

    return values


def get_column_positions(line, column_names):
    """
    Diese Funktion liest die Positionen der Spaltennamen in der ersten Zeile der txt-Datei und gibt sie zurück.
    """
    columns = clean_and_split_line(line)

    value_position = {}

    for column_name in column_names:
        value_position[column_name] = -1
    for i in range(len(columns)):
        for column_name in value_position:
            if columns[i] == column_name:
                value_position[column_name] = i