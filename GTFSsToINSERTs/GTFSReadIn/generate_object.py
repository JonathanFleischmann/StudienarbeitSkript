import sys
import os
from data_storage import DataTable
from GTFSReadIn.core import clean_and_split_line, get_value_position_map_from_array, shorten_values_to_relevant, get_ids_from_filename

def generate_table_object_from_filepath(filepath, filename, stop_thread_var):
    """
    Diese Funktion liest die angegebene Datei und schreibt die Daten in ein Table-Objekt mit der id als key.
    :param filepath: Der Pfad zur Datei, die gelesen werden soll
    :param filename: Der Name der Datei, die gelesen wird
    :param stop_thread_var: Ein tkinter-Boolean-Objekt, das angibt, ob der Prozess abgebrochen wurde
    :return: Ein Table-Objekt, das die Daten aus der Datei enthält
    """
    try:
        # Erhalte die Gesamtgröße der Datei
        total_size = os.path.getsize(filepath)
        calculate_progress = False

        if total_size >= 50000000:
            calculate_progress = True
            with open(filepath, "r", encoding="utf-8-sig") as file:
                sample_lines = [file.readline() for _ in range(300)]
                avg_line_length_utf8 = sum(len(line.encode('utf-8')) for line in sample_lines) / len(sample_lines)

        # Öffne die Datei im Lesemodus
        with open(filepath, "r", encoding="utf-8-sig") as file:
            # Lese die Namen der Spaltennamen in der ersten Zeile der txt-Datei in ein Array ein
            columns = clean_and_split_line(file.readline())

            # erstelle eine Map, die angibt, wo die relevanten Spalten in der txt-Datei liegen
            # und überprüfe, ob alle notwendigen Werte vorhanden sind
            column_position = shorten_values_to_relevant(
                get_value_position_map_from_array(columns), 
                filename
            )

            # finde den Index der id-Spalte(n)
            id_positions = []
            # füge die indexe der id-Spalten in ein Array ein
            # wenn es nur eine id-Spalte gibt, lösche sie aus der column_position-Map 
            # -> kommt dadurch nur noch als key und nicht als value in der DataTable vor
            id_array = get_ids_from_filename(filename)
            for column in column_position:
                if column in id_array:
                    id_positions.append(column_position[column])
                    if len(id_array) <= 1:
                        del column_position[column]
                        break
                    
            # überprüfe, ob alle id-Spalten gefunden wurden
            if len(id_positions) > len(id_array):
                print("Es wurden mehr id-Spalten gefunden, als für die Datei \"" + filename + "\" erwartet. Dies muss ein Logikfehler in der Programmierung sein", file=sys.stderr)
                sys.exit(1)
            if len(id_positions) < len(id_array):
                ids_not_found = [id for id in id_array if id not in column_position]
                print("Die id-Spalten \"" + ids_not_found + "\" wurden nicht gefunden.", file=sys.stderr)
                sys.exit(1)

            # Erstelle ein Table-Objekt, das die Daten speichert
            table = DataTable(filename, list(column_position))

            # Gehe alle Zeilen in der txt-Datei ab der zweiten Zeile durch
            linecount = 0
            processed_size = 0

            for line in file:
                # Überprüfe, ob der Prozess abgebrochen werden soll
                if linecount % 1000 == 0 and stop_thread_var.get(): return

                # Aktualisiere die verarbeitete Dateigröße
                if calculate_progress:
                    processed_size += avg_line_length_utf8

                    if linecount % 10000 == 0:
                        progress = (processed_size / total_size) * 100
                        print(f"\rDatei **{filename}**: geschätzter Fortschritt: {progress:.2f}%")

                # Bereinige die Zeile und teile sie in Werte auf
                values = clean_and_split_line(line)

                # Schreibe die relevanten Werte in das Table-Objekt mit der id als key
                id = ""
                for id_position in id_positions:
                    id += str(values[id_position])
                relevant_values = []
                for column, position in column_position.items():
                    relevant_values.append(values[position])
                table.add_record(id, relevant_values)

                linecount += 1
            return table
    
    except Exception as e:
        print(f"Fehler beim Verarbeiten der Datei **{filepath}**: {e}", file=sys.stderr)