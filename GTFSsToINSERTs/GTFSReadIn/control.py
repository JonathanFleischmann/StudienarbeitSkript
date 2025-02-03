import sys
from GTFSReadIn.preset_values import necessary_values, relevant_values, id_values

def shorten_file_map_to_relevant_files(filemap):
    """
    Diese Funktion überprüft, ob alle notwendigen Dateien vorhanden sind und gibt eine Map mit den relevanten Dateien zurück.
    """

    # Erstelle eine Map mit den Namen der Dateien und Pfaden zu den relevanten txt-Dateien
    relevant_files = {}

    # Überprüfe, ob alle notwendigen Dateien vorhanden sind
    for filename in necessary_values.keys():
        # Wenn notwendige Datei nicht vorhanden ist, werfe einen Fehler und beende das Programm
        if not filename in filemap:
            print(f"Die Datei {filename}.txt wurde nicht gefunden.", file=sys.stderr)
            sys.exit(1)

    # Füge alle relevanten Dateien zur relevant_files-Map hinzu
    for filename in relevant_values.keys():
        if filename in filemap:
            relevant_files[filename] = filemap[filename]
        
    return relevant_files

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
     Diese Funktion überprüft, ob alle notwendigen Werte in der txt-Datei vorhanden sind 
     und kürzt die Werte in der txt-Datei auf die relevanten Werte.
    """

    if filename in necessary_values:
        for value in necessary_values[filename]:
            if value not in value_position:
                print(f"Die Spalte {value} wurde nicht in der Datei {filename}.txt gefunden.", file=sys.stderr)
                sys.exit(1)

    shortened_values = {}
    for value in relevant_values[filename]:
        if value in value_position:
            shortened_values[value] = value_position[value]
    return shortened_values

def get_id_name_from_filename(filename):
    """
    Diese Funktion gibt den Namen der id-Spalte für die Datei zurück.
    """
    if filename in id_values:
        return id_values[filename]
    else:
        print(f"Die id-Spalte für die Datei {filename}.txt wurde nicht gefunden.", file=sys.stderr)
        sys.exit(1)