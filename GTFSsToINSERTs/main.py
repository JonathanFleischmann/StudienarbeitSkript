from GTFSReadIn.GTFSReadIn import get_table_map_from_GTFSs
from ExecuteInserts.execute_inserts import execute_inserts
from database_connection import get_db_connection

def main():
    # lese die GTFS-Datensätze in Objekte ein und speichere diese anhand ihrer Namen in eine Map
    conn = get_db_connection()

    gtfs_tables = get_table_map_from_GTFSs()

    print("\n\n")

    execute_inserts(gtfs_tables, conn)


    # bilde die eingelesenen GTFS-Tabellen auf die Datenbank-Tabellen ab





# Wenn das Skript direkt ausgeführt wird, rufe die main-Funktion auf
if __name__ == "__main__":
    main()
