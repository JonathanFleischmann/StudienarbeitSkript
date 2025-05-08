import sqlite3
from preset_values import column_names_map

def clear_traffic_point_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    
    old_table_name = "stops"
    new_table_name = "traffic_point"

    # Erhalte die Spalten der Tabelle 'stops' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'stops' in 'traffic_point'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")

    for column in old_table_columns:
        if column not in column_names_map[new_table_name]:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} DROP COLUMN {column};")
        elif column_names_map[new_table_name][column][0] != column:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN {column} TO {column_names_map[new_table_name][column][0]};")
    for sql in table_edit_sql:
        cache_db.execute(sql)
    cache_db.commit()

    if "parent_station" in old_table_columns:
        
        # Suche nach Einträgen in der Tabelle 'traffic_point', die als 'traffic_centre_id' einen 'traffic_point' haben,
        # welcher wiederum eine Referenz auf eine 'traffic_centre_id' hat und löse diese auf
        select_traffic_point_references_in_traffic_centre_sql = f"""
            SELECT DISTINCT s2.traffic_centre_id, s1.record_id 
            FROM {new_table_name} s1
            JOIN {new_table_name} s2 ON s1.traffic_centre_id = s2.record_id
            WHERE s2.traffic_centre_id != s1.record_id
            LIMIT {batch_size} OFFSET ?
        """
        # Erstelle das Update-Statement, um die Daten in die neue Tabelle 'traffic_point' zu übertragen
        update_traffic_point_sql = f"UPDATE {new_table_name} SET traffic_centre_id = :1 WHERE record_id = :2"

        offset = 0
        while True:
            if stop_thread_var.get(): return

            # Hole die Kombinationen aus record_ids und traffic_centre_ids aus der traffic_point-Tabelle
            traffic_point_references = cache_db.execute(select_traffic_point_references_in_traffic_centre_sql, (offset,)).fetchall()
            # Wenn keine weiteren Einträge gefunden wurden, breche die Schleife ab
            if len(traffic_point_references) == 0:
                break
            # Wandle die Kombinationen in eine Liste von Tupeln um [(traffic_centre, record_id)]
            traffic_point_references = [(str(traffic_centre), record_id) for traffic_centre, record_id in traffic_point_references]
            # Führe das Update-Statement aus
            cache_db.executemany(update_traffic_point_sql, traffic_point_references)
            # committe die Änderungen
            cache_db.commit()
            # Erhöhe den Offset um die Anzahl der gefundenen Einträge
            offset += len(traffic_point_references)

        # Ersetze die 'parent_station' in der Spalte 'traffic_centre_id' in der cache-DB durch die neu generierte ID der 'traffic_centre'-Tabelle
        select_ids_sql = f"SELECT id, record_id FROM traffic_centre LIMIT {batch_size} OFFSET ?"
        # Erstelle dazu eine neue Spalte, um die IDs zu speichern und Referenzen auf parent_stations, die nicht als traffic_centre 
        # erkannt wurden, zu löschen
        cache_db.execute(f"ALTER TABLE {new_table_name} RENAME COLUMN traffic_centre_id TO parent_station;")
        cache_db.commit()
        # Füge die neue Spalte 'traffic_centre_id' hinzu, um die IDs zu speichern
        cache_db.execute(f"ALTER TABLE {new_table_name} ADD COLUMN traffic_centre_id TEXT;")
        cache_db.commit()
        update_id_sql = f"UPDATE {new_table_name} SET traffic_centre_id = :1 WHERE parent_station = :2"

        total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM traffic_centre").fetchone()[0]

        for i in range(0, total_update_conditions, batch_size):
            if stop_thread_var.get(): return

            # Hole die Kombinationen aus ids und record_ids aus der traffic_centre-Tabelle
            traffic_centre_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
            # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
            traffic_centre_ids = [(str(id), record_id) for id, record_id in traffic_centre_ids]
            # Führe das Update-Statement aus
            cache_db.executemany(update_id_sql, traffic_centre_ids)
            # committe die Änderungen
            cache_db.commit()

        # Lösche die Spalte 'parent_station' aus der Tabelle 'traffic_point', da sie nicht mehr benötigt wird
        cache_db.execute(f"ALTER TABLE {new_table_name} DROP COLUMN parent_station;")
        cache_db.commit()


    # Ersetze die 'location_type' in der Spalte 'location_type' in der cache-DB durch die neu generierte ID der 'location_type'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM location_type LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET location_type_id = :1 WHERE location_type_id = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM location_type").fetchone()[0]

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