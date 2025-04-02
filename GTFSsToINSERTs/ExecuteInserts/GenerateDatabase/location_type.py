import sqlite3

def create_new_location_type_cache_db_table(cache_db: sqlite3.Connection):

    # Definiere den Namen der neuen Tabelle
    new_table_name = "location_type"
    # Erstelle eine neue Tabelle 'location_type' in der Datenbank
    create_location_type_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {new_table_name} (
        record_id TEXT PRIMARY KEY,
        type TEXT
    );
    """
    cache_db.execute(create_location_type_table_sql)
    cache_db.commit()

    # Erstelle eine Liste von Spalten, die in der neuen Tabelle 'location_type' verwendet werden sollen
    new_table_columns = ["record_id", "type"]

    # Erstelle das Insert-Statement, um die Daten in die neue Tabelle 'location_type' zu übertragen
    insert_sql = f"INSERT INTO {new_table_name} ({', '.join(new_table_columns)}) VALUES ({', '.join(['?'] * len(new_table_columns))})"

    # Erstelle eine Liste von vordefinierten Werten für die Tabelle 'location_type'
    predefined_values = [
        ("0", "platform"),
        ("1", "station"),
        ("2", "entrance/exit"),
        ("3", "generic node"),
        ("4", "boarding area")
    ]

    # Füge die vordefinierten Werte in die neue Tabelle 'location_type' ein
    cache_db.executemany(insert_sql, predefined_values)
    # committe die Änderungen
    cache_db.commit()