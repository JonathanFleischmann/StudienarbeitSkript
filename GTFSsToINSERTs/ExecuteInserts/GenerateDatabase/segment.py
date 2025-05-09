import sqlite3
from preset_values import column_names_map

def clear_segment_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    old_table_name = "stop_times"
    new_table_name = "segment"

    # Erhalte die Spalten der Tabelle 'stop_times' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    # Erhalte die Spalten der Tabelle 'trip' aus der Datenbank
    trip_columns = cache_db.execute(f"PRAGMA table_info(trip)").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    trip_columns = [column[1] for column in trip_columns]


    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'stop_times' in 'segment'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")
    # Übertrage die Spaltennamen aus der Tabelle 'stop_times' in die neue Tabelle 'segment'
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN stop_sequence TO sequence;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN stop_id TO start_point;")
    table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN end_point TEXT;")
    # Die Spalten 'trip_id', 'arrival_time' und 'departure_time' werden in der Tabelle 'segment' nicht umbenannt, 
    # da sie bereits die richtigen Namen haben
    if "stop_headsign" in old_table_columns:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN stop_headsign TO destination;")
    # wenn die Spalte 'stop_headsign' in der Tabelle 'stop_times' nicht vorhanden ist,
    # aber die Spalte 'headsign' in der Tabelle 'trip' vorhanden ist, dann füge die Spalte 'destination' hinzu
    else:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN destination TEXT;")
    if "pickup_type" in old_table_columns:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN pickup_type TO enter_type;")
    if "drop_off_type" in old_table_columns:
        table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN drop_off_type TO descend_type;")

    for sql in table_edit_sql:
        cache_db.execute(sql)
    cache_db.commit()

    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_segment_trip_id_sequence ON {new_table_name} (trip_id, sequence);")
    cache_db.commit()

    # Ersetze die 'trip_id' in der Tabelle 'segment' mit der ID der 'trip'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM trip LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET trip_id = :1 WHERE trip_id = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM trip").fetchone()[0]

    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der trip-Tabelle
        trip_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        trip_ids = [(str(id), record_id) for id, record_id in trip_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, trip_ids)
        # committe die Änderungen
        cache_db.commit()



    # Fülle die Spalte 'end_point' in der Tabelle 'segment' mit den Werten aus der Spalte 'start_point' aus dem Datensatz mit der
    # nächsthöheren 'sequence' und dem gleichen Wert in der Spalte 'trip_id' in der Tabelle 'segment'
    # und die Spalte 'arrival_time' in der Tabelle 'segment' mit den Werten aus der Spalte 'arrival_time' aus dem Datensatz mit der
    # nächsthöheren 'sequence' und dem gleichen Wert in der Spalte 'trip_id' in der Tabelle 'segment'
    # Fülle die Spalte 'descend_type' in der Tabelle 'segment' mit den Werten aus der Spalte 'descend_type' aus dem Datensatz mit der
    # nächsthöheren 'sequence' und dem gleichen Wert in der Spalte 'trip_id' in der Tabelle 'segment'

    print(f"\rFülle die Spalten 'end_point', 'arrival_time' und 'descend_type' in der Tabelle '{new_table_name}' aus - Dies kann einige Minuten dauern... ")

    # TODO: könnte Probleme geben, da kein Alias für die Tabelle 'segment' verwendet wird
    update_sql = f"""
        UPDATE {new_table_name} AS recent_row
        SET 
            end_point = (
                SELECT start_point 
                FROM {new_table_name} AS next_row
                WHERE next_row.trip_id = recent_row.trip_id
                AND CAST(next_row.sequence AS INTEGER) = CAST(recent_row.sequence AS INTEGER) + 1
            ),
            arrival_time = (
                SELECT arrival_time
                FROM {new_table_name} AS next_row
                WHERE next_row.trip_id = recent_row.trip_id
                AND CAST(next_row.sequence AS INTEGER) = CAST(recent_row.sequence AS INTEGER) + 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM {new_table_name} AS next_row
            WHERE next_row.trip_id = recent_row.trip_id
            AND CAST(next_row.sequence AS INTEGER) = CAST(recent_row.sequence AS INTEGER) + 1
        );
        """
    cache_db.execute(update_sql)
    cache_db.commit()

    if "drop_off_type" in old_table_columns:
        update_sql = f"""
        UPDATE {new_table_name} AS recent_row
        SET
            descend_type = (
                SELECT descend_type
                FROM {new_table_name} AS next_row
                WHERE next_row.trip_id = recent_row.trip_id
                AND CAST(next_row.sequence AS INTEGER) = CAST(recent_row.sequence AS INTEGER) + 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM {new_table_name} AS next_row
            WHERE next_row.trip_id = recent_row.trip_id
            AND CAST(next_row.sequence AS INTEGER) = CAST(recent_row.sequence AS INTEGER) + 1
        );
        """
    cache_db.execute(update_sql)
    cache_db.commit()

    print(f"\rLösche die letzten Datensätze mit der höchsten 'sequence' für jeden 'trip_id' in der Tabelle '{new_table_name}' - Dies kann einige Minuten dauern... ")

    # Lösche jeweils den letzten Datensatz mit der höchsten 'sequence' für jeden 'trip_id' in der Tabelle 'segment'
    delete_sql = f"""
    DELETE FROM {new_table_name} WHERE end_point IS NULL;
    """
    cache_db.execute(delete_sql)
    cache_db.commit()

    print(f"\rErsetze die 'enter_type' und 'descend_type' in der Tabelle '{new_table_name}' mit der ID der 'stop_type'-Tabelle - Dies kann einige Minuten dauern... ")

    # Ersetze die 'enter_type' und 'descend_type' in der Tabelle 'segment' mit der ID der 'stop_type'-Tabelle
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

    print(f"\rErsetze die 'start_point' und 'end_point' in der Tabelle '{new_table_name}' mit der ID der 'traffic_point'-Tabelle - Dies kann einige Minuten dauern... ")

    # Ersetze die 'start_point' und 'end_point' in der Tabelle 'segment' mit der ID der 'traffic_point'-Tabelle
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


    # Erstelle Indexe für die Tabelle 'trip'
    cache_db.execute("CREATE INDEX IF NOT EXISTS idx_trip_id ON trip (id);")

    # Erstelle Indexe für die Tabelle 'traffic_point'
    cache_db.execute("CREATE INDEX IF NOT EXISTS idx_traffic_point_id ON traffic_point (id);")

    # Erstelle Indexe für die Tabelle 'segment'
    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_segment_trip_id ON {new_table_name} (trip_id);")
    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_segment_end_point ON {new_table_name} (end_point);")
    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_segment_record_id_sequence_trip_id ON {new_table_name} (record_id, sequence, trip_id);")

    # Committe die Änderungen
    cache_db.commit()

    print(f"\rFülle die Spalte 'destination' in der Tabelle '{new_table_name}' aus - Dies kann einige Minuten dauern... ")

    # Fülle die Spalte 'destination' in der Tabelle 'segment' aus.
    # Wenn die Spalte 'stop_headsign' in der Tabelle 'stop_times' bereits vorhanden war, dann fülle nur die Werte, die NULL oder '' sind
    # in der Spalte 'destination' in der Tabelle 'segment', ansonsten fülle die gesamte Spalte 'destination' in der Tabelle 'segment'
    # Nutze dafür wenn vorhanden die Werte aus der Spalte 'trip_headsign' in der Tabelle 'trip' mit der entsprechenden 'trip' in der 
    # Tabelle 'segment'
    # Wenn nicht vorhanden, dann suche den Eintrag in der Tabelle 'segment' mit der höchsten 'sequence' und dem gleichen 'trip_id' und
    # hole den Wert der Spalte 'name' aus der Tabelle 'traffic_point', bei der die id der 'traffic_point' mit der id der 'end_point'
    # in der Tabelle 'segment' übereinstimmt
     
    if "trip_headsign" in trip_columns:
        update_sql = f"""
            UPDATE {new_table_name}
            SET destination = (
                SELECT trip_headsign
                FROM trip AS t
                WHERE t.id = {new_table_name}.trip_id
            )
            WHERE destination IS NULL OR destination = '';
        """
    else:
        update_sql = f"""
            UPDATE {new_table_name}
            SET destination = (
                SELECT tp.name
                FROM traffic_point AS tp
                WHERE tp.id = (
                    SELECT end_point
                    FROM {new_table_name} AS s
                    WHERE s.trip_id = {new_table_name}.trip_id
                    AND s.sequence = (
                        SELECT MAX(sequence)
                        FROM {new_table_name} AS s2
                        WHERE s2.trip_id = {new_table_name}.trip_id
                    )
                )
            )
            WHERE destination IS NULL OR destination = '';
        """
    cache_db.execute(update_sql)
    cache_db.commit()

    # Lösche die Spalte 'trip_headsign' in der Tabelle 'trip'
    if "trip_headsign" in trip_columns:
        cache_db.execute(f"ALTER TABLE trip DROP COLUMN trip_headsign;")
        cache_db.commit()

    print(f"\r")
