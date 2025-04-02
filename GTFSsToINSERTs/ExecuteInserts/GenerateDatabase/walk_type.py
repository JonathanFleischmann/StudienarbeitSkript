import sqlite3

def create_new_walk_type_cache_db_table(cache_db: sqlite3.Connection) -> None:
    # Definiere den Namen der neuen Tabelle
    new_table_name = "walk_type"
    # Erstelle eine neue Tabelle 'walk_type' in der Datenbank
    create_walk_type_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {new_table_name} (
        record_id TEXT PRIMARY KEY,
        type TEXT
    );
    """
    cache_db.execute(create_walk_type_table_sql)
    cache_db.commit()

    # Erstelle eine Liste von Spalten, die in der neuen Tabelle 'walk_type' verwendet werden sollen
    new_table_columns = ["record_id", "type"]

    # Erstelle das Insert-Statement, um die Daten in die neue Tabelle 'walk_type' zu übertragen
    insert_sql = f"INSERT INTO {new_table_name} ({', '.join(new_table_columns)}) VALUES ({', '.join(['?'] * len(new_table_columns))})"

    # Erstelle eine Liste von vordefinierten Werten für die Tabelle 'walk_type'
    predefined_values = [
        ("1", "walkway"),
        ("2", "stairs"),
        ("3", "moving sidewalk/travelator"),
        ("4", "escalator"),
        ("5", "elevator"),
        ("6", "fare gate"),
        ("7", "exit gate")
    ]

    # Füge die vordefinierten Werte in die neue Tabelle 'walk_type' ein
    cache_db.executemany(insert_sql, predefined_values)
    # committe die Änderungen
    cache_db.commit()