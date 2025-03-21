import sys
import csv
import os
from preset_values import necessary_gtfs_files_and_attributes, relevant_gtfs_files_and_attributes, id_values

def get_txt_files_in_path(folder_path):
    """
    Diese Funktion listet alle .txt-Dateien im angegebenen Ordner auf und gibt sie in einer Map zurück,
    wobei die Dateiendung .txt vom Key entfernt wird.
    :param folder_path: Der Pfad zum Ordner, in dem die .txt-Dateien liegen
    :return: Eine Map, die den Dateinamen ohne Erweiterung auf den vollständigen Pfad abbildet
    :raises: SystemExit, wenn ein Fehler beim Auflisten der Dateien auftritt
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


def shorten_file_map_to_relevant_files(filemap):
    """
    Diese Funktion überprüft, ob alle notwendigen Dateien vorhanden sind und gibt eine Map mit den relevanten Dateien zurück.
    """

    # Erstelle eine Map mit den Namen der Dateien und Pfaden zu den relevanten txt-Dateien
    relevant_files = {}

    # Überprüfe, ob alle notwendigen Dateien vorhanden sind
    for filename in necessary_gtfs_files_and_attributes.keys():
        # Wenn notwendige Datei nicht vorhanden ist, werfe einen Fehler und beende das Programm
        if not filename in filemap:
            print(f"Die Datei {filename}.txt wurde nicht gefunden.", file=sys.stderr)
            sys.exit(1)

    # Füge alle relevanten Dateien zur relevant_files-Map hinzu
    for filename in relevant_gtfs_files_and_attributes.keys():
        if filename in filemap:
            relevant_files[filename] = filemap[filename]
        
    return relevant_files


def clean_and_split_line(line):
    """
    Diese Funktion entfernt führende und nachfolgende Leerzeichen, entfernt Leerzeichen zwischen
    den Werten, entfernt äußere Anführungszeichen um die Werte und teilt die Zeile anhand von Kommas.
    Ausnahme: 
    - Kommas innerhalb von Anführungszeichen werden nicht als Trennzeichen interpretiert.
    - Anführungszeichen innerhalb der Werte bleiben erhalten.
    :param line: Die Zeile, die bereinigt und geteilt werden soll
    :return: Eine Liste mit den Werten der Zeile
    """
    # Verwende csv.reader, um Kommas innerhalb von Anführungszeichen korrekt zu behandeln
    reader = csv.reader([line], quotechar='"', skipinitialspace=True)
    
    # Die csv.reader gibt einen Iterator zurück, wir nehmen das erste (und einzige) Element
    values = next(reader)

    return values


def get_value_position_map_from_array(columns):
    """
    Diese Funktion erstellt eine Map, die angibt, wo die Spaltennamen in der txt-Datei liegen.
    """
    value_position = {}
    for i in range(len(columns)):
        value_position[columns[i]] = i
    return value_position
    

def shorten_values_to_relevant(value_position, filename):
    """
    Diese Funktion überprüft, ob alle notwendigen Werte für die txt-Datei vorhanden sind 
    und kürzt die Werte in der txt-Datei auf die relevanten Werte.
    """

    if filename in necessary_gtfs_files_and_attributes:
        for value in necessary_gtfs_files_and_attributes[filename]:
            if value not in value_position:
                print(f"Die Spalte {value} wurde nicht in der Datei {filename}.txt gefunden.", file=sys.stderr)
                sys.exit(1)

    shortened_values = {}
    for value in relevant_gtfs_files_and_attributes[filename]:
        if value in value_position:
            shortened_values[value] = value_position[value]
    return shortened_values


def get_ids_from_filename(filename):
    """
    Diese Funktion gibt den Namen der id-Spalte für die Datei zurück.
    """
    if filename in id_values:
        return id_values[filename]
    else:
        print(f"Die id-Spalte für die Datei {filename}.txt wurde nicht gefunden.", file=sys.stderr)
        sys.exit(1)