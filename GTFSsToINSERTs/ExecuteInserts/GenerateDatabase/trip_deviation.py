import sqlite3
from preset_values import column_names_map

def create_new_trip_deviation_cache_db_table(cache_db: sqlite3.Connection, batch_size: int, stop_thread_var) -> None:

    # TODO: WICHTIG: Spalte 'exception_type' Kann auch eine positive Ausnahme sein -> doch relevant

    internal_batch_size = batch_size * 20

    new_table_name = "trip_deviation"

    # Erstelle eine neue Tabelle 'trip_deviation' in der Datenbank
    create_trip_deviation_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {new_table_name} (
        record_id TEXT PRIMARY KEY,
        trip_id TEXT,
        deviation_id TEXT
    );
    """
    cache_db.execute(create_trip_deviation_table_sql)
    cache_db.commit()

    # Füge die 'id' in 'trip' und die 'id' in 'deviation' als 'trip_id' und 'deviation_id' in die neue Tabelle 
    # 'trip_deviation' ein, wo die 'service_id' in der Tabelle 'trip' und 'deviation_id' übereinstimmt
    # generiere die record_id für die neue Tabelle 'trip_deviation' aus der Kombination der 'id' in 'trip' 
    # und der 'id' in 'deviation'
    select_ids_sql = f"""
    SELECT trp.id, dev.id FROM trip AS trp 
    JOIN deviation AS dev ON trp.service_id = dev.service_id
    LIMIT {internal_batch_size} OFFSET ?
    """
    insert_sql = f"INSERT INTO {new_table_name} (record_id, trip_id, deviation_id) VALUES (?, ?, ?)"
    
    number_of_rows_sql = f"SELECT COUNT(*) FROM trip AS trp JOIN deviation AS dev ON trp.service_id = dev.service_id"
    number_of_rows = cache_db.execute(number_of_rows_sql).fetchone()[0]

    offset = 0

    print(f"Erstelle die Tabelle '{new_table_name}' und füge die Daten ein...")

    id_number = 0

    while True:
        if stop_thread_var.get(): return

        # Hole die Kombinationen von ids aus der trip- und deviation-Tabelle
        trip_deviation_ids = cache_db.execute(select_ids_sql, (offset,)).fetchall()

        if not trip_deviation_ids:
            break

        # Wandle die Kombinationen in eine Liste von Tupeln um [(record_id, trip_id, deviation_id)]
        index = 0
        for trip_id, deviation_id in trip_deviation_ids:
            id_number += 1
            # Erstelle die record_id aus der Kombination der 'id' in 'trip' und der 'id' in 'deviation'
            record_id = str(id_number)
            trip_deviation_ids[index] = (record_id, trip_id, deviation_id)
            index += 1

        # Füge die Daten in die neue Tabelle 'trip_deviation' ein
        cache_db.executemany(insert_sql, trip_deviation_ids)

        # committe die Änderungen
        cache_db.commit()

        offset += internal_batch_size

        print(f"\r Fortschritt bei der Erstellung der trip_deviation: {(offset/number_of_rows)*100:.2f}%", end="")

    
    print("\r\r")



    
    # Lösche die beiden Spalten 'service_id' in der Tabelle 'trip' und 'exception_table'
    delete_sql = f"""
    ALTER TABLE trip DROP COLUMN service_id;
    ALTER TABLE deviation DROP COLUMN service_id;
    """
    cache_db.executescript(delete_sql)
    cache_db.commit()