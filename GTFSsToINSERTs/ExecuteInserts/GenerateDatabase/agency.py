import sqlite3
from preset_values import column_names_map

def clear_agency_cache_db_table(cache_db: sqlite3.Connection) -> None:
    old_table_name = "agency"
    new_table_name = "agency"

    # Erhalte die Spalten der Tabelle 'agency' aus der Datenbank
    old_table_columns = cache_db.execute("PRAGMA table_info(agency);").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
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


