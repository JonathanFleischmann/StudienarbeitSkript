import sqlite3
from preset_values import column_names_map

def clear_period_cache_db_table(cache_db: sqlite3.Connection, batch_size, stop_thread_var) -> None:
    old_table_name = "calendar"
    new_table_name = "period"
    
    # Erhalte die Spalten der Tabelle 'routes' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'calendar' in 'period'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")
    # Füge die Spalte 'weekdays' hinzu
    table_edit_sql.append(f"ALTER TABLE {new_table_name} ADD COLUMN weekdays_id TEXT;")

    for column in old_table_columns:
        if column not in column_names_map[new_table_name]:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} DROP COLUMN {column};")
        elif column_names_map[new_table_name][column][0] != column:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN {column} TO {column_names_map[new_table_name][column][0]};")
    # führe die SQL-Statements aus
    for sql in table_edit_sql:
        cache_db.execute(sql)
    # committe die Änderungen
    cache_db.commit()

    # Ersetze die 'weekdays' in der Spalte 'weekdays' in der cache-DB durch die neu generierte ID der 'weekdays'-Tabelle
    select_ids_sql = f"SELECT id, record_id FROM weekdays LIMIT {batch_size} OFFSET ?"

    update_id_sql = f"UPDATE {new_table_name} SET weekdays_id = :1 WHERE record_id = :2"

    total_update_conditions = cache_db.execute(f"SELECT COUNT(*) FROM weekdays").fetchone()[0]

    # Setze die Foreign-Keys auf die 'weekdays' Spalte
    for i in range(0, total_update_conditions, batch_size):
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der weekdays-Tabelle
        weekdays_ids = cache_db.execute(select_ids_sql, (i,)).fetchall()
        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        weekdays_ids = [(str(id), record_id) for id, record_id in weekdays_ids]
        # Führe das Update-Statement aus
        cache_db.executemany(update_id_sql, weekdays_ids)
        # committe die Änderungen
        cache_db.commit()
