import sqlite3
from preset_values import column_names_map

def create_new_ride_exception_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:

    # TODO: WICHTIG: Spalte 'exception_type' Kann auch eine positive Ausnahme sein -> doch relevant

    new_table_name = "ride_exception"

    # Erstelle eine neue Tabelle 'ride_exception' in der Datenbank
    create_ride_exception_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {new_table_name} (
        record_id TEXT PRIMARY KEY,
        ride TEXT,
        exception_table TEXT
    );
    """
    cache_db.execute(create_ride_exception_table_sql)
    cache_db.commit()

    # Füge die 'id' in 'ride' und die 'id' in 'exception_table' als 'ride' und 'exception_table' in die neue Tabelle 
    # 'ride_exception' ein, wo die 'service_id' in der Tabelle 'ride' und 'exception_table' übereinstimmt
    # generiere die record_id für die neue Tabelle 'ride_exception' aus der Kombination der 'id' in 'ride' 
    # und der 'id' in 'exception_table'
    select_ids_sql = f"""
    SELECT rd.id, et.id FROM ride AS rd 
    JOIN exception_table AS et ON rd.service_id = et.service_id
    LIMIT {batch_size} OFFSET ?
    """
    insert_sql = f"INSERT INTO {new_table_name} (record_id, ride, exception_table) VALUES (?, ?, ?)"
    
    offset = 0
    while True:
        if stop_thread_var.get(): return

        # Hole die Kombinationen aus ids und record_ids aus der ride- und exception_table-Tabelle
        ride_exception_ids = cache_db.execute(select_ids_sql, (offset,)).fetchall()
        if not ride_exception_ids:
            break

        # Wandle die Kombinationen in eine Liste von Tupeln um [(id, record_id)]
        ride_exception_ids = [(str(ride_id) + str(exception_table_id), ride_id, exception_table_id) 
                              for ride_id, exception_table_id in ride_exception_ids]

        # Füge die Daten in die neue Tabelle 'ride_exception' ein
        cache_db.executemany(insert_sql, ride_exception_ids)

        # committe die Änderungen
        cache_db.commit()

        offset += batch_size

    
    # Lösche die beiden Spalten 'service_id' in der Tabelle 'ride' und 'exception_table'
    delete_sql = f"""
    ALTER TABLE ride DROP COLUMN service_id;
    ALTER TABLE exception_table DROP COLUMN service_id;
    """
    cache_db.executescript(delete_sql)
    cache_db.commit()