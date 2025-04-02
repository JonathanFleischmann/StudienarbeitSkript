from GTFSReadIn.core import get_txt_files_in_path, shorten_file_map_to_relevant_files
from GTFSReadIn.generate_object import add_gtfs_table_to_db_from_filepath
import sqlite3

def get_database_cache_from_GTFSs(cache_db, input_folder, stop_thread_var)-> None:

    # Erstelle eine map mit den Dateinamen und Pfaden zu den txt-Dateien
    txt_files = shorten_file_map_to_relevant_files(get_txt_files_in_path(input_folder))

    for filename in txt_files:
        print("Verarbeite Datei: **" + filename + "**")
        add_gtfs_table_to_db_from_filepath(txt_files[filename], filename, cache_db, stop_thread_var)
        if stop_thread_var.get(): return
        print("\r ✅ Datei **" + filename + "** wurde erfolgreich verarbeitet.")

    # Erstelle Indexe auf die cache-DB
    # CREATE INDEX IF NOT EXISTS idx_stops_parent_station ON stops(parent_station);
    # CREATE INDEX IF NOT EXISTS idx_stops_stop_id ON stops(stop_id);
    # CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id);

    cache_db.execute("CREATE INDEX IF NOT EXISTS idx_stops_parent_station ON stops(parent_station);")
    cache_db.execute("CREATE INDEX IF NOT EXISTS idx_stops_stop_id ON stops(stop_id);")
    cache_db.execute("CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id);")

    cache_db.commit()

