import sqlite3
from preset_values import column_names_map

def clear_exception_table_cache_db_table(cache_db: sqlite3.Connection) -> None:
    old_table_name = "calendar_dates"
    new_table_name = "exception_table"

    # Erhalte die Spalten der Tabelle 'calendar_dates' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'calendar_dates' in 'exception_table'
    table_edit_sql.append(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name};")

    for column in old_table_columns:
        if column not in column_names_map[new_table_name]:
            if column != "service_id":
                table_edit_sql.append(f"ALTER TABLE {new_table_name} DROP COLUMN {column};")
        elif column_names_map[new_table_name][column][0] != column:
            table_edit_sql.append(f"ALTER TABLE {new_table_name} RENAME COLUMN {column} TO {column_names_map[new_table_name][column][0]};")
    for sql in table_edit_sql:
        cache_db.execute(sql)
    cache_db.commit()