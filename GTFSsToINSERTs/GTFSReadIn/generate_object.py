import sys

from GTFSReadIn.file_read import clean_and_split_line
from GTFSReadIn.data_storage import GTFSTable
from GTFSReadIn.control import get_value_position_map_from_array, shorten_values_to_relevant, get_id_name_from_filename


def generate_table_object_from_filepath(filepath, filename):
    """
    Diese Funktion liest die angegebene Datei und schreibt die Daten in ein Table-Objekt mit der id als key.
    
    filepath: Der Dateipfad zur .txt-Datei.
    """
    try:
        # Öffne die Datei im Lesemodus
        with open(filepath, "r", encoding="utf-8-sig") as file:

            # Lese die Namen der Spaltennamen in der ersten Zeile der txt-Datei in ein Array ein
            columns = clean_and_split_line(file.readline())

            # erstelle eine Map, die angibt, wo die relevanten Spalten in der txt-Datei liegen
            # und überprüfe, ob alle notwendigen Werte vorhanden sind
            attribute_position = shorten_values_to_relevant(
                get_value_position_map_from_array(columns), 
                filename
            )

            # finde den Index der id-Spalte

            id_positions = []
            # gehe jedes Element in der Map durch und suche die id-Spalte(n)
            id_array = get_id_name_from_filename(filename)
            for attribute in attribute_position:
                if len(id_array) <= 1:
                    if id_array[0] == attribute:
                        id_positions.append(attribute_position[attribute])
                        del attribute_position[attribute]
                        break
                else:
                    for id_name in id_array:
                        if id_name in attribute:
                            id_positions.append(attribute_position[attribute])
            
            if len(id_positions) == 0:	
                print("Die id-Spalten \"" + id_array + "\" wurden nicht gefunden.", file=sys.stderr)
                sys.exit(1)

            # Erstelle ein Table-Objekt, das die Daten speichert
            table = GTFSTable(list(attribute_position), filename)

            # Gehe alle Zeilen in der txt-Datei ab der zweiten Zeile durch
            for line in file:
                # Bereinige die Zeile und teile sie in Werte auf
                values = clean_and_split_line(line)

                # Schreibe die relevanten Werte in das Table-Objekt mit der id als key
                id = ""
                for id_position in id_positions:
                    id += str(values[id_position])
                relevant_values = []
                for attribute in attribute_position:
                    relevant_values.append(values[attribute_position[attribute]])
                table.add_record(id, relevant_values)

            return table
    
    except Exception as e:
        print(f"Fehler beim Verarbeiten der Datei {filepath}: {e}", file=sys.stderr)