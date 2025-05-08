import sqlite3
import sys
from preset_values import column_names_map

def clear_walk_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:
    old_table_name = "pathways"
    new_table_name = "walk"

    # Erhalte die Spalten der Tabelle 'stop_times' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    is_bidirectional_in_walk = "is_bidirectional" in old_table_columns
    walk_type_in_walk = "traversal_time" in old_table_columns

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'routes' in 'route'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")

    for column in old_table_columns:
        if column not in column_names_map[new_table_name]:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} DROP COLUMN {column};")
        elif column_names_map[new_table_name][column][0] != column:
            if column != "is_bidirectional":
                table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN {column} TO {column_names_map[new_table_name][column][0]};")
    # führe die SQL-Statements aus
    for sql in table_edit_sql:
        cache_db.execute(sql)
    # committe die Änderungen
    cache_db.commit()

    
    
    # Ersetze die 'start_point' und 'end_point' in der Tabelle 'walk' mit der ID der 'traffic_point'-Tabelle
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

    

    
    # Ersetze die 'walk_type_id' in der Tabelle 'walk' mit der ID der 'walk_type'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM walk_type LIMIT {batch_size} OFFSET ?"
    update_id_sql = f"UPDATE {new_table_name} SET walk_type_id = :1 WHERE walk_type_id = :2"

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



    # Wenn die Spalte 'is_bidirectional' in der Tabelle 'walk' existiert, suche nach Datensätzen, die 'is_bidirectional' = 1 haben
    # und dupliziere diese Datensätze, wobei 'is_bidirectional' = 0 gesetzt wird und die 'start_point' und 'end_point' vertauscht werden

    if is_bidirectional_in_walk:
        
        print(f"\r Suche nach bidirektionalen Wegen in der Tabelle '{new_table_name}' - Dies kann einige Minuten dauern")

        # Führe das Insert-Statement aus, um die Duplikate zu erstellen
        if walk_type_in_walk:
            insert_sql = f"""
            INSERT INTO walk (start_point, end_point, walk_type_id, is_bidirectional, traversal_time)
            SELECT end_point, start_point, walk_type_id, 0, traversal_time
            FROM walk
            WHERE is_bidirectional = 1
            """
        else:
            insert_sql = f"""
            INSERT INTO walk (start_point, end_point, is_bidirectional, traversal_time)
            SELECT end_point, start_point, 0, traversal_time
            FROM walk
            WHERE is_bidirectional = 1
            """
        # Führe das Insert-Statement aus
        cache_db.execute(insert_sql)
        # committe die Änderungen
        cache_db.commit()

        # Lösche die Spalte 'is_bidirectional' aus der Tabelle 'walk'
        cache_db.execute(f"ALTER TABLE {new_table_name} DROP COLUMN is_bidirectional;")
        # committe die Änderungen
        cache_db.commit()
   
