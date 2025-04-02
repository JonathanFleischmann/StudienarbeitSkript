import sqlite3
from preset_values import column_names_map

def create_new_weekdays_cache_db_table(cache_db: sqlite3.Connection, batch_size, stop_thread_var)-> None:
    old_table_name = "calendar"
    new_table_name = "weekdays"

    # Erstelle eine neue Tabelle 'weekdays' in der Datenbank
    create_weekdays_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {new_table_name} (
        record_id TEXT PRIMARY KEY,
        monday TEXT,
        tuesday TEXT,
        wednesday TEXT,
        thursday TEXT,
        friday TEXT,
        saturday TEXT,
        sunday TEXT
    );
    """
    cache_db.execute(create_weekdays_table_sql)
    cache_db.commit()

    # Erstelle eine Liste von Spalten, die in der neuen Tabelle 'weekdays' verwendet werden sollen
    new_table_columns = list(column_names_map[new_table_name].keys())

    # Erstelle das Select-Statement, um die Daten aus der alten Tabelle 'calendar' zu holen
    select_sql = f"SELECT {', '.join(new_table_columns)} FROM {old_table_name} LIMIT {batch_size} OFFSET ?"

    # Erstelle das Insert-Statement, um die Daten in die neue Tabelle 'weekdays' zu übertragen
    insert_sql = f"INSERT INTO {new_table_name} ({', '.join(new_table_columns)}) VALUES ({', '.join(['?'] * len(new_table_columns))})"

    # Hole die Anzahl der Zeilen in der alten Tabelle 'calendar'
    total_rows = cache_db.execute(f"SELECT COUNT(*) FROM {old_table_name}").fetchone()[0]

    # Iteriere über die Zeilen in der alten Tabelle 'calendar' und füge sie in die neue Tabelle 'weekdays' ein
    for i in range(0, total_rows, batch_size):
        if stop_thread_var.get(): return

        # Hole die Daten aus der alten Tabelle 'calendar' -> Ergebnis ist eine Liste von Tupeln
        # [(record_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday)]
        rows = cache_db.execute(select_sql, (i,)).fetchall()

        # Füge die Daten in die neue Tabelle 'weekdays' ein
        cache_db.executemany(insert_sql, rows)

        # committe die Änderungen
        cache_db.commit()