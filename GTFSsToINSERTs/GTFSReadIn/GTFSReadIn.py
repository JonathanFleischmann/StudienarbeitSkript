from GTFSReadIn.control import shorten_file_map_to_relevant_files
from GTFSReadIn.file_read import get_txt_files_in_path, get_folder
from GTFSReadIn.generate_object import generate_table_object_from_filepath

def get_table_map_from_GTFSs(input_folder):

    # Erstelle eine map mit den Dateinamen und Pfaden zu den txt-Dateien
    txt_files = shorten_file_map_to_relevant_files(get_txt_files_in_path(input_folder))

    # Erstelle eine Map mit den Table-Objekten für jede txt-Datei
    table_objects = {}   
    for filename in txt_files:
        table_objects[filename] = generate_table_object_from_filepath(txt_files[filename], filename)
        print("Verarbeite Datei:", filename)

    return table_objects
