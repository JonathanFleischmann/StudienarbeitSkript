import sqlite3
import time
from preset_values import column_names_map
from ExecuteInserts.core import show_progress

def clear_trip_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    old_table_name = "trips"
    new_table_name = "trip"

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
                table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN period_id TEXT;")
            else:
                table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN {column} TO {column_names_map[new_table_name][column][0]};")
    for sql in table_edit_sql:
        cache_db.execute(sql)
    cache_db.commit()

    # Ersetze die 'route_id' in der Spalte 'route_id' in der cache-DB durch die neu generierte ID der 'route'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM route LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET route_id = :1 WHERE route_id = :2"

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

    # Füge die neu generierte ID der 'period'-Tabelle für die 'service_id' in die Spalte 'period_id' in der cache-DB hinzu
    select_ids_sql = f"SELECT id, record_id FROM period LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET period_id = :1 WHERE service_id = :2"

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
    
    # Weise jedem trip die Startzeit zu
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

    
    # Hole alle Einträge, die trotzdem keine Startzeit haben und lösche sie und ihre Einträge in der Tabelle 'stop_times'
    select_trip_ids_sql = f"SELECT record_id FROM {new_table_name} WHERE start_time IS NULL LIMIT {batch_size} OFFSET ?"
    delete_trip_sql = f"DELETE FROM {new_table_name} WHERE record_id = :1"
    delete_stop_times_sql = f"DELETE FROM stop_times WHERE trip_id = :1"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM {new_table_name} WHERE start_time IS NULL").fetchone()[0]
    start_time = time.time()

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die record_ids aus der trips-Tabelle, bei denen die Startzeit NULL ist
        trip_ids = cache_db.execute(select_trip_ids_sql, (i,)).fetchall()
        # Wandle die record_ids in eine Liste von Tupeln um [(record_id)]
        trip_ids = [(record_id[0],) for record_id in trip_ids]
        # Führe das Delete-Statement aus
        cache_db.executemany(delete_trip_sql, trip_ids)
        cache_db.executemany(delete_stop_times_sql, trip_ids)
        # committe die Änderungen
        cache_db.commit()

        if total_update_conditions > batch_size:
            show_progress(i + batch_size, total_update_conditions, start_time, "\n trip-Einträge ohne valide segments löschen")
