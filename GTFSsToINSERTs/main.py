from GTFSReadIn.GTFSReadIn import get_table_map_from_GTFSs
from ExecuteInserts.execute_inserts import execute_inserts
from database_connection import get_db_connection
from UserInput.user_interface import start_user_interface

def main():
    # lese die GTFS-Datensätze in Objekte ein und speichere diese anhand ihrer Namen in eine Map
    
    config_data = start_user_interface()

    if not config_data:
        print("Keine Konfigurationsdaten eingegeben.")
        return
    
    conn = get_db_connection(
        config_data["host"], 
        config_data["port"], 
        config_data["service_name"], 
        config_data["username"], 
        config_data["password"]
    )

    gtfs_tables = get_table_map_from_GTFSs(
        config_data["gtfs_path"]
    )

    print("\n\n")

    execute_inserts(gtfs_tables, conn)


    # bilde die eingelesenen GTFS-Tabellen auf die Datenbank-Tabellen ab





# Wenn das Skript direkt ausgeführt wird, rufe die main-Funktion auf
if __name__ == "__main__":
    main()
