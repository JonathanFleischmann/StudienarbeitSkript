# import os
# import sys

# from ExecuteInserts.file_read import clean_and_split_line
# from ExecuteInserts.data_storage import DatabaseTable
# from Skripts.GTFSsToINSERTs.ExecuteInserts.core import get_value_position_map_from_array, shorten_values_to_relevant
# from ExecuteInserts.preset_values import height_attribute_names_replace_map


# def generate_height_table_object_from_filepath(filepath):
#     """
#     Diese Funktion liest die angegebene Datei und schreibt die Daten in ein Table-Objekt mit der id als key.
    
#     filepath: Der Dateipfad zur .txt-Datei.
#     """
#     try:
#         # Öffne die Datei im Lesemodus
#         with open(filepath, "r", encoding="utf-8-sig") as file:

#             # Lese die Namen der Spaltennamen in der ersten Zeile der txt-Datei in ein Array ein
#             columns = clean_and_split_line(file.readline())

#             # erstelle eine Map, die angibt, wo die relevanten Spalten in der txt-Datei liegen
#             # und überprüfe, ob alle notwendigen Werte vorhanden sind
#             attribute_position = shorten_values_to_relevant(
#                 get_value_position_map_from_array(columns), 
#                 "levels"
#             )

#             # finde den Index der id-Spalte

#             id_position = -1
#             # gehe jedes Element in der Map durch und finde das, welches "agency_id" lautet
#             for attribute in attribute_position:
#                 if "level_id" in attribute:
#                     id_position = attribute_position[attribute]
#                     del attribute_position[attribute]
#                     break
            
#             if id_position == -1:
#                 print("Die id-Spalte \"level_id\" wurde nicht gefunden.", file=sys.stderr)
#                 sys.exit(1)

#             # Ersetze die Attributnamen in der Map durch die Namen, unter denen sie in der Datenbank gespeichert werden
#             new_attributes_position = {}
#             for attribute in attribute_position:
#                 if attribute in height_attribute_names_replace_map:
#                     # trage in der neuen Map die Position unter dem richhtigen Namen ein
#                     new_attributes_position[height_attribute_names_replace_map[attribute]] = attribute_position[attribute]
#                 else:
#                     print(f"Das Attribut {attribute} wurde nicht in height_attribute_names_replace_map gefunden.", file=sys.stderr)
#                     sys.exit(1)
#             attribute_position = new_attributes_position

#             # Erstelle ein Table-Objekt, das die Daten speichert
#             table = DatabaseTable(list(attribute_position))

#             # Gehe alle Zeilen in der txt-Datei ab der zweiten Zeile durch
#             for line in file:
#                 # Bereinige die Zeile und teile sie in Werte auf
#                 values = clean_and_split_line(line)

#                 # Schreibe die relevanten Werte in das Table-Objekt mit der id als key
#                 id = values[id_position]
#                 relevant_values = []
#                 for attribute in attribute_position:
#                     relevant_values.append(values[attribute_position[attribute]])
#                 table.add_record(id, relevant_values)

#             return table
    
#     except Exception as e:
#         print(f"Fehler beim Verarbeiten der Datei {filepath}: {e}", file=sys.stderr)