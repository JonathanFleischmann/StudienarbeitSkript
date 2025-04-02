import sqlite3
from preset_values import column_names_map

def clear_path_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    old_table_name = "stop_times"
    new_table_name = "path"

    # Erhalte die Spalten der Tabelle 'stop_times' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    distination_in_ride: bool = False

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'stop_times' in 'path'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")
    # Übertrage die Spaltennamen aus der Tabelle 'stop_times' in die neue Tabelle 'path'
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN trip_id TO ride;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN stop_sequence TO sequence;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN stop_id TO start_point;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN end_point TEXT;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN arrival_time TO arrival_time;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN departure_time TO departure_time;")
    if "stop_headsign" in old_table_columns:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN stop_headsign TO destination;")
    # wenn die Spalte 'stop_headsign' in der Tabelle 'stop_times' nicht vorhanden ist,
    # aber die Spalte 'headsign' in der Tabelle 'ride' vorhanden ist, dann füge die Spalte 'destination' hinzu
    else:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN destination TEXT;")
        distination_in_ride = True
    if "pickup_type" in old_table_columns:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN pickup_type TO enter_type;")
    if "drop_off_type" in old_table_columns:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN drop_off_type TO descend_type;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN is_ride TEXT DEFAULT '1';")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN min_travel_time TEXT;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN walk_type TEXT;")

    for sql in table_edit_sql:
        cache_db.execute(sql)
    cache_db.commit()

    # Fülle die Spalte 'end_point' in der Tabelle 'path' mit den Werten aus der Spalte 'start_point' aus dem Datensatz mit der
    # nächsthöheren 'stop_sequence' und dem gleichen Wert in der Spalte 'ride' in der Tabelle 'path'
    # und die Spalte 'arrival_time' in der Tabelle 'path' mit den Werten aus der Spalte 'arrival_time' aus dem Datensatz mit der
    # nächsthöheren 'stop_sequence' und dem gleichen Wert in der Spalte 'ride' in der Tabelle 'path'
    update_sql = f"""
        UPDATE {new_table_name}
        SET 
            end_point = (
                SELECT start_point 
                FROM {new_table_name} AS next_row
                WHERE next_row.ride = {new_table_name}.ride
                AND CAST(next_row.sequence AS INTEGER) = CAST({new_table_name}.sequence AS INTEGER) + 1
            ),
            arrival_time = (
                SELECT arrival_time
                FROM {new_table_name} AS next_row
                WHERE next_row.ride = {new_table_name}.ride
                AND CAST(next_row.sequence AS INTEGER) = CAST({new_table_name}.sequence AS INTEGER) + 1
            )
        WHERE end_point IS NULL
        AND EXISTS (
            SELECT 1
            FROM {new_table_name} AS next_row
            WHERE next_row.ride = {new_table_name}.ride
            AND CAST(next_row.sequence AS INTEGER) = CAST({new_table_name}.sequence AS INTEGER) + 1
        );
        """
    cache_db.execute(update_sql)
    cache_db.commit()


    # Fülle die Spalte 'descend_type' in der Tabelle 'path' mit den Werten aus der Spalte 'descend_type' aus dem Datensatz mit der
    # nächsthöheren 'stop_sequence' und dem gleichen Wert in der Spalte 'ride' in der Tabelle 'path'
    if "drop_off_type" in old_table_columns:
        update_sql = f"""
        UPDATE {new_table_name}
        SET 
            descend_type = (
                SELECT descend_type
                FROM {new_table_name} AS next_row
                WHERE next_row.ride = {new_table_name}.ride
                AND CAST(next_row.sequence AS INTEGER) = CAST({new_table_name}.sequence AS INTEGER) + 1
            )
        WHERE end_point IS NULL
        AND EXISTS (
            SELECT 1
            FROM {new_table_name} AS next_row
            WHERE next_row.ride = {new_table_name}.ride
            AND CAST(next_row.sequence AS INTEGER) = CAST({new_table_name}.sequence AS INTEGER) + 1
        );
        """
        cache_db.execute(update_sql)
        cache_db.commit()

    # update die Spalte 'min_travel_time' in der Tabelle 'path' mit den Werten aus der Spalte 'arrival_time' 
    # minus der Spalte 'departure_time' in Minuten aus der gleichen Zeile in der Tabelle 'path'
    # und setze dann die Spalten 'arrival_time' und 'departure_time' in ein 24h Format (HH:MM:SS)
    # liegen im Format HH:MM:SS vor, gehen aber über 24h hinaus
    update_sql = f"""
        UPDATE {new_table_name}
        SET
            min_travel_time = 
                CAST((strftime('%H', arrival_time) * 60 + strftime('%M', arrival_time)) AS INTEGER) -
                CAST((strftime('%H', departure_time) * 60 + strftime('%M', departure_time)) AS INTEGER),
            arrival_time =
                CASE
                    WHEN CAST(strftime('%H', arrival_time) AS INTEGER) >= 24 THEN
                        strftime('%H:%M:%S', arrival_time, '-24 hours')
                    ELSE
                        arrival_time
                END,
            departure_time =
                CASE
                    WHEN CAST(strftime('%H', departure_time) AS INTEGER) >= 24 THEN
                        strftime('%H:%M:%S', departure_time, '-24 hours')
                    ELSE
                        departure_time
                END
        WHERE arrival_time IS NOT NULL AND departure_time IS NOT NULL;
    """
    cache_db.execute(update_sql)
    cache_db.commit()

    # Fülle die Spalte 'destination' in der Tabelle 'path' mit den Werten aus der Spalte 'headsign' aus der Tabelle 'ride'
    # mit der entsprechenden 'ride' in der Tabelle 'path' wenn 'headsign' in der Tabelle 'ride' vorhanden ist aber 
    # 'stop_headsign' in der Tabelle 'stop_times' nicht vorhanden war
    if distination_in_ride:
        update_sql = f"""
            UPDATE {new_table_name}
            SET destination = (
                SELECT headsign
                FROM ride AS r
                WHERE r.record_id = {new_table_name}.ride
            )
            WHERE destination IS NULL;
        """
        cache_db.execute(update_sql)
        cache_db.commit()

    # lösche die Datensätze, die die höchste 'stop_sequence' für jeden 'ride' in der Tabelle 'path' haben
    delete_sql = f"""
        DELETE FROM {new_table_name}
        WHERE record_id IN (
            SELECT record_id
            FROM {new_table_name} AS t1
            WHERE t1.stop_sequence = (
                SELECT MAX(t2.stop_sequence)
                FROM {new_table_name} AS t2
                WHERE t2.ride = t1.ride
            )
        );
    """
    cache_db.execute(delete_sql)
    cache_db.commit()

    # Ersetze die 'ride' in der Tabelle 'path' mit der ID der 'ride'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM ride LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET ride = :1 WHERE ride = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM ride").fetchone()[0]

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der ride-Tabelle
        ride_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        ride_ids = [(str(id), record_id) for id, record_id in ride_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, ride_ids)
        # committe die Änderungen
        cache_db.commit()

    # Ersetze die 'enter_type' und 'descend_type' in der Tabelle 'path' mit der ID der 'stop_type'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM stop_type LIMIT {batch_size} OFFSET ?"
    update_id_sql_enter = f"UPDATE {new_table_name} SET enter_type = :1 WHERE enter_type = :2"
    update_id_sql_descend = f"UPDATE {new_table_name} SET descend_type = :1 WHERE descend_type = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM stop_type").fetchone()[0]

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der stop_type-Tabelle
        stop_type_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        stop_type_ids = [(str(id), record_id) for id, record_id in stop_type_ids]
        # Führe das Update-Statement für enter_type aus
        cache_db.executemany(update_id_sql_enter, stop_type_ids)
        # committe die Änderungen
        cache_db.commit()
        # Führe das Update-Statement für descend_type aus
        cache_db.executemany(update_id_sql_descend, stop_type_ids)
        # committe die Änderungen
        cache_db.commit()
    
    # Finde heraus, ob die Tabelle 'pathways' in der Datenbank vorhanden ist
    pathways_exists = cache_db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='pathways'").fetchone()
    if pathways_exists is not None:

        # Füge die Datensätze aus der Tabelle 'pathways' in die Tabelle 'path' ein
        # Setze die Spalten 'ride', 'sequence', 'departure_time', 'arrival_time' und 'destination' nicht
        # 'is_ride' auf 0, 'min_travel_time' auf den Wert aus der Spalte 'traversal_time',
        # 'walk_type' auf den Wert aus der Spalte 'pathway_mode', 'start_point' auf den Wert aus der Spalte 'from_stop_id'
        # und 'end_point' auf den Wert aus der Spalte 'to_stop_id' in der Tabelle 'pathways'
        # und dupliziere die Zeile und tausche 'start_point' und 'end_point' aus, wenn 'is_bidirectional' = 1

        # Erhalte die Spalten der Tabelle 'pathways' aus der Datenbank
        pathways_gtfs_table_columns = cache_db.execute(f"PRAGMA table_info(pathways)").fetchall()
        # Konvertiere die Spalten in eine Liste von Strings
        pathways_gtfs_table_columns = [column[1] for column in pathways_gtfs_table_columns]

        # finde heraus, ob die Spalte 'traversal_time' in der Tabelle 'pathways' vorhanden ist
        traversal_time_in_pathways: bool = "traversal_time" in pathways_gtfs_table_columns
        if traversal_time_in_pathways:
            # Erstelle die SQL-Statements, die die Datensätze aus der Tabelle 'pathways' in die Tabelle 'path' entsprechend übertragen
            # wenn die Spalte 'traversal_time' in der Tabelle 'pathways' vorhanden ist (also min_travel_time gesetzt werden kann)
            insert_sql = f"""
                INSERT INTO {new_table_name} (is_ride, min_travel_time, walk_type, start_point, end_point)
                SELECT 0, traversal_time, pathway_mode, from_stop_id, to_stop_id
                FROM pathways
                WHERE is_bidirectional = 1;
            """
        else:
            # Erstelle die SQL-Statements, die die Datensätze aus der Tabelle 'pathways' in die Tabelle 'path' entsprechend übertragen
            # wenn die Spalte 'traversal_time' in der Tabelle 'pathways' nicht vorhanden ist (also min_travel_time nicht gesetzt werden kann)
            insert_sql = f"""
                INSERT INTO {new_table_name} (is_ride, walk_type, start_point, end_point)
                SELECT 0, pathway_mode, from_stop_id, to_stop_id
                FROM pathways
                WHERE is_bidirectional = 1;
            """
        cache_db.execute(insert_sql)
        cache_db.commit()
        
        # Finde heraus, ob die Spalte 'is_bidirectional' in der Tabelle 'pathways' vorhanden ist
        is_bidirectional_in_pathways: bool = "is_bidirectional" in pathways_gtfs_table_columns
        if is_bidirectional_in_pathways:
            # Erstelle die SQL-Statements, die die Datensätze aus der Tabelle 'pathways' in die Tabelle 'path' entsprechend übertragen
            # wenn die Spalte 'is_bidirectional' in der Tabelle 'pathways' vorhanden ist (also die Zeile dupliziert werden kann)
            if traversal_time_in_pathways:
                insert_sql = f"""
                    INSERT INTO {new_table_name} (is_ride, min_travel_time, walk_type, start_point, end_point)
                    SELECT 0, traversal_time, pathway_mode, to_stop_id, from_stop_id
                    FROM pathways
                    WHERE is_bidirectional = 1;
                """
            else:
                insert_sql = f"""
                    INSERT INTO {new_table_name} (is_ride, min_travel_time, walk_type, start_point, end_point)
                    SELECT 0, traversal_time, pathway_mode, to_stop_id, from_stop_id
                    FROM pathways
                    WHERE is_bidirectional = 1;
                """
            cache_db.execute(insert_sql)
            cache_db.commit()

    # Ersetze die 'start_point' und 'end_point' in der Tabelle 'path' mit der ID der 'traffic_point'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM traffic_point LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET start_point = :1 WHERE start_point = :2"
    update_id_sql_end = f"UPDATE {new_table_name} SET end_point = :1 WHERE end_point = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM traffic_point").fetchone()[0]

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der traffic_point-Tabelle
        traffic_point_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        traffic_point_ids = [(str(id), record_id) for id, record_id in traffic_point_ids]
        # Führe das Update-Statement für start_point aus
        cache_db.executemany(update_id_sql, traffic_point_ids)
        # committe die Änderungen
        cache_db.commit()
        # Führe das Update-Statement für end_point aus
        cache_db.executemany(update_id_sql_end, traffic_point_ids)
        # committe die Änderungen
        cache_db.commit()

    # Ersetze die 'walk_type' in der Tabelle 'path' mit der ID der 'walk_type'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM walk_type LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET walk_type = :1 WHERE walk_type = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM walk_type").fetchone()[0]

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der walk_type-Tabelle
        walk_type_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        walk_type_ids = [(str(id), record_id) for id, record_id in walk_type_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, walk_type_ids)
        # committe die Änderungen
        cache_db.commit()