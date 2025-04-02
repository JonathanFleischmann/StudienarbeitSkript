import sqlite3

def create_new_stop_type_cache_db_table(cache_db: sqlite3.Connection) -> None:
    # Define den Namen der neuen Tabelle
    new_table_name = "stop_type"
    # Erstelle die Tabelle 'stop_type' in der Datenbank
    create_stop_type_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {new_table_name} (
        record_id TEXT PRIMARY KEY,
        type TEXT
    );"""
    cache_db.execute(create_stop_type_table_sql)
    cache_db.commit()

    # Erstelle eine Liste von Spalten, die in der neuen Tabelle 'stop_type' verwendet werden sollen
    new_table_columns = ["record_id", "type"]

    # Erstelle das Insert-Statement, um die Daten in die neue Tabelle 'stop_type' zu übertragen
    insert_sql = f"INSERT INTO {new_table_name} ({', '.join(new_table_columns)}) VALUES ({', '.join(['?'] * len(new_table_columns))})"

    # Erstelle eine Liste von vordefinierten Werten für die Tabelle 'stop_type'
    predefined_values = [
        ("0", "regular stop"),
        ("1", "no service"),
        ("2", "must phone agency to arrange service"),
        ("3", "must coordinate with driver to arrange service")
    ]

    # Füge die vordefinierten Werte in die neue Tabelle 'stop_type' ein
    cache_db.executemany(insert_sql, predefined_values)
    # committe die Änderungen
    cache_db.commit()