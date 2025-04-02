import sys
import sqlite3
from preset_values import column_names_map

def create_new_traffic_centre_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    old_table_name = "stops"
    new_table_name = "traffic_centre"

    print(f" Traffic-Centres werden aus der Tabelle '{old_table_name}' in die Tabelle '{new_table_name}' extrahiert.")

    # Erhalte durch die Spalten der Tabelle 'stops' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    new_table_columns = []
    used_old_table_columns = []

    for column in old_table_columns:
        if column in column_names_map[new_table_name]:
            used_old_table_columns.append(column)
            new_table_columns.append(column_names_map[new_table_name][column][0])
    
    # Erstelle die neue Tabelle 'traffic_centre' mit den neuen Spalten
    create_table_sql = f"CREATE TABLE {new_table_name} ("
    for column in new_table_columns:
        if column == 'record_id':
            create_table_sql += f"{column} TEXT PRIMARY KEY, "
        else:
            create_table_sql += f"{column} TEXT, "
    create_table_sql = create_table_sql[:-2] + ");"

    cache_db.execute(create_table_sql)
    cache_db.commit()

    # Überprüfe, ob die Spalte 'parent_station' vorhanden ist
    if "parent_station" not in old_table_columns:
        print(f"\rEs wurden keine Verknüpfungen zu zentralen Verkehrsknotenpunkten gefunden", file=sys.stderr)
        
        return

    # Erstelle das Insert-Statement, um die Daten in die neue Tabelle 'traffic_centre' zu übertragen    
    insert_sql = f"INSERT INTO {new_table_name} ({', '.join(new_table_columns)}) VALUES ({', '.join(['?'] * len(new_table_columns))})"

    # Erstelle ein Delete-Statement, um die Daten aus der alten Tabelle 'stops' zu löschen
    delete_sql = f"DELETE FROM {old_table_name} WHERE record_id = ?"

    # Erstelle das Select-Statement, um die gesuchten Daten aus der cache-DB zu holen:
    #   Finde in der Tabelle 'stops' alle Einträge, die parent_stations von anderen Einträgen sind,
    #   selbst aber keine parent_station haben und auf die keine Referenz von der Tabelle 'stop_times' existiert

    select_sql = f"""
        SELECT {', '.join([f's1.{col}' for col in used_old_table_columns])} 
        FROM {old_table_name} s1
        WHERE (s1.parent_station IS NULL OR s1.parent_station = '')
        AND EXISTS (
            SELECT 1 FROM {old_table_name} s2 WHERE s2.parent_station = s1.stop_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM stop_times st WHERE st.stop_id = s1.stop_id
        )
    """


    print(f"\r Suche nach zentralen Verkehrsknotenpunkten in der Tabelle '{old_table_name}' - Dies kann einige Minuten dauern", file=sys.stderr)

    if stop_thread_var.get(): return

    # Hole die Daten aus der alten Tabelle 'stops' -> Ergebnis ist eine Liste von Tupeln
    # [(record_id, stop_name, stop_lat, ...)]
    rows = cache_db.execute(select_sql).fetchall()

    if rows:
        print(f"\r Breakpoint: {len(rows)} zentrale Verkehrsknotenpunkte gefunden", file=sys.stderr)

        # Füge die Datensätze in die neue Tabelle 'traffic_centre' ein
        cache_db.executemany(insert_sql, rows)
        
        # Lösche die Datensätze aus der alten Tabelle 'stops'
        # Erstelle eine Liste von record_ids, die gelöscht werden sollen
        record_ids_to_delete = [row[0] for row in rows]
        # Führe das Delete-Statement aus
        cache_db.executemany(delete_sql, [(record_id,) for record_id in record_ids_to_delete])

        # committe die Änderungen
        cache_db.commit()

    print(f"\r Suche nach zentralen Verkehrsknotenpunkten in der Tabelle '{old_table_name}' abgeschlossen", file=sys.stderr)

    # Ersetze den 'location_type' in der Spalte 'location_type' in der cache-DB durch die neu generierte ID der 'location_type'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM location_type LIMIT {batch_size} OFFSET ?"

    update_id_sql = f"UPDATE {new_table_name} SET location_type = :1 WHERE location_type = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM location_type").fetchone()[0]

    print(f"\r Updaten der Fremdschlüssel in der Tabelle '{new_table_name}'", file=sys.stderr)

    cache_db.execute("PRAGMA synchronous = OFF")
    cache_db.execute("PRAGMA journal_mode = MEMORY")
    cache_db.execute("BEGIN TRANSACTION")

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der location_type-Tabelle
        location_type_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        location_type_ids = [(str(id), record_id) for id, record_id in location_type_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, location_type_ids)
        # committe die Änderungen
        cache_db.commit()

    print("\r")