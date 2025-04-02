import sqlite3
import time
from preset_values import column_names_map
from ExecuteInserts.core import show_progress

def clear_ride_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    old_table_name = "trips"
    new_table_name = "ride"

    # Erhalte die Spalten der Tabelle 'trips' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]
    
    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'trips' in 'ride'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")

    for column in old_table_columns:
        if column not in column_names_map[new_table_name]:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} DROP COLUMN {column};")
        elif column_names_map[new_table_name][column][0] != column:
            if column == "service_id":
                table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN period TEXT;")
            else:
                table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN {column} TO {column_names_map[new_table_name][column][0]};")
    for sql in table_edit_sql:
        cache_db.execute(sql)
    cache_db.commit()

    # Ersetze die 'route_id' in der Spalte 'route' in der cache-DB durch die neu generierte ID der 'route'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM route LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET route = :1 WHERE route = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM route").fetchone()[0]
    start_time = time.time()

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der route-Tabelle
        route_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        route_ids = [(str(id), record_id) for id, record_id in route_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, route_ids)
        # committe die Änderungen
        cache_db.commit()

        show_progress(i + batch_size, total_update_conditions, start_time, "Route-IDs aktualisieren")

    # Füge die neu generierte ID der 'period'-Tabelle für die 'service_id' in die Spalte 'period' in der cache-DB hinzu
    select_ids_sql = f"SELECT id, record_id FROM period LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET period = :1 WHERE service_id = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM period").fetchone()[0]
    start_time = time.time()

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der period-Tabelle
        period_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        period_ids = [(str(id), record_id) for id, record_id in period_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, period_ids)
        # committe die Änderungen
        cache_db.commit()

        show_progress(i + batch_size, total_update_conditions, start_time, "Period-IDs aktualisieren")

    
    # Füge die Spalte 'start_time' hinzu
    cache_db.execute(f"ALTER TABLE {new_table_name} ADD COLUMN start_time TEXT;")
    cache_db.commit()
    
    # Weise jedem Ride die Startzeit zu
    # Hole dazu die Spalten 'departure_time' und 'trip_id' aus der Tabelle 'stop_times' wo 'stop_sequence' = 1
    # und füge die Startzeit in die Spalte 'start_time' der Tabelle 'ride' ein wo 'record_id' = 'trip_id'
    select_start_time_sql = f"SELECT departure_time, trip_id FROM stop_times WHERE stop_sequence = 1 LIMIT {batch_size} OFFSET ?"
    update_start_time_sql = f"UPDATE {new_table_name} SET start_time = :1 WHERE record_id = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM stop_times WHERE stop_sequence = 1").fetchone()[0]
    start_time = time.time()

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus departure_time und trip_id aus der stop_times-Tabelle
        start_time_ids = cache_db.execute(select_start_time_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(departure_time, trip_id)]
        start_time_ids = [(str(departure_time), trip_id) for departure_time, trip_id in start_time_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_start_time_sql, start_time_ids)
        # committe die Änderungen
        cache_db.commit()

        show_progress(i + batch_size, total_update_conditions, start_time, "Startzeiten aktualisieren")

    # Füge die Spalte 'headsign' hinzu, wenn noch nicht vorhanden
    if "trip_headsign" not in old_table_columns:
        cache_db.execute(f"ALTER TABLE {new_table_name} ADD COLUMN headsign TEXT;")
        cache_db.commit()
    
    # Finde die letzte Haltestelle für jeden Trip und weise diese demm 'headsign' zu, wenn der 'headsign' leer ist
    # Hole dazu die Spalten 'stop_id' und 'trip_id' aus der Tabelle 'stop_times' wo 'stop_sequence' = (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = trip_id)
    # finde zu dieser 'stop_id' den 'name' aus der Tabelle 'traffic_point' und füge den 'name' in die Spalte 'headsign' der Tabelle 'ride' ein wo 'record_id' = 'trip_id'
    # joine dazu die Tabelle 'stop_times' mit der Tabelle 'traffic_point' mit der Bedingung 'stop_id' = 'record_id' 
    # und 'stop_sequence' = (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = trip_id) und 
    # joine die Tabelle 'ride' mit der Tabelle 'stop_times' mit der Bedingung 'record_id' = 'trip_id' 
    # und 'headsign' = ""
    select_headsign_sql = f"""
        SELECT tp.name, st.trip_id FROM stop_times st
        JOIN traffic_point tp ON st.stop_id = tp.record_id
        WHERE st.stop_sequence = (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = st.trip_id)
        AND st.trip_id IN (SELECT record_id FROM {new_table_name} WHERE headsign IS NULL OR headsign = "")
        LIMIT {batch_size} OFFSET ?
    """
    update_headsign_sql = f"UPDATE {new_table_name} SET headsign = :1 WHERE record_id = :2"

    total_update_conditions = cache_db.execute(f"""
        SELECT COUNT(*) FROM stop_times st
        JOIN traffic_point tp ON st.stop_id = tp.record_id
        WHERE st.stop_sequence = (SELECT MAX(stop_sequence) FROM stop_times WHERE trip_id = st.trip_id)
        AND st.trip_id IN (SELECT record_id FROM {new_table_name} WHERE headsign IS NULL OR headsign = "")
    """).fetchone()[0]
    start_time = time.time()

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return
        headsign_ids = cache_db.execute(select_headsign_sql, (i,)).fetchall()
        if not headsign_ids:
            break
        headsign_ids = [(str(name), trip_id) for name, trip_id in headsign_ids]
        cache_db.executemany(update_headsign_sql, headsign_ids)
        cache_db.commit()
        show_progress(i + batch_size, total_update_conditions, start_time, "Headsigns aktualisieren")