import sqlite3
from preset_values import column_names_map
import datetime

def clear_deviation_cache_db_table(cache_db: sqlite3.Connection) -> None:
    old_table_name = "calendar_dates"
    new_table_name = "deviation"

    # Erhalte die Spalten der Tabelle 'calendar_dates' aus der Datenbank
    old_table_columns = cache_db.execute(f"PRAGMA table_info({old_table_name})").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings
    old_table_columns = [column[1] for column in old_table_columns]

    # erstelle die SQL-Statements, die die Spalten in der Tabelle entsprechend anpassen
    table_edit_sql = []
    # Ändere den Namen der Tabelle 'calendar_dates' in 'deviation'
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


def append_trip_with_period_and_weekdays_if_deviation_positive(
    cache_db: sqlite3.Connection,
    stop_thread_var
) -> None:
    old_table_name = "calendar_dates"

    print(f"Extrahiere positive Abweichungen in der Tabelle {old_table_name} - Dies kann einige Minuten dauern...")

    # Hole service_id und date von allen Einträgen aus der Tabelle 'calendar_dates' mit einem Wert von 1 in der Spalte 'exception_type' geordnet nach service_id und date
    select_sql = f"SELECT service_id, date FROM {old_table_name} WHERE exception_type = 1 ORDER BY service_id, date"

    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_calendar_dates_service_id_date ON {old_table_name} (service_id, date);")
    cache_db.commit()

    # Wandle die Daten so um, dass alle dates einer service_id in einem Array gespeichert werden, welches sich in einer map befindet, bei der der key die service_id ist
    # und der value ein Array mit den dates ist

    service_id_dates_map = {}
    for row in cache_db.execute(select_sql):
        service_id = row[0]
        date = row[1]
        if service_id not in service_id_dates_map:
            service_id_dates_map[service_id] = []
        service_id_dates_map[service_id].append(date)

    # Erstelle eine weitere Map mit service_id als key, die als value das dictionary {start_date: string, end_date: string, deviations: string[]} hat
    # wobei start_date und end_date die ersten und letzten dates in der Liste sind und deviations die dates sind, die in der Liste zwischen den anderen Daten fehlen

    service_id_dicts_map = {}

    for service_id, dates in service_id_dates_map.items():
        if stop_thread_var.get(): return

        # Hole das erste und letzte Datum
        start_date_string = dates[0]
        end_date_string = dates[-1]
        # die Daten liegen im Format YYYYMMDD vor -> wandle sie in datetime.date-Objekte um
        start_date = datetime.date(int(start_date_string[:4]), int(start_date_string[4:6]), int(start_date_string[6:]))
        end_date = datetime.date(int(end_date_string[:4]), int(end_date_string[4:6]), int(end_date_string[6:]))
        # Hole die Abweichungen
        deviations = []
        current_date = start_date
        while current_date <= end_date:
            # Wenn das aktuelle Datum nicht in der Liste der Daten ist, füge es der Liste der Abweichungen hinzu
            if current_date.strftime("%Y%m%d") not in dates:
                deviations.append(current_date.strftime("%Y%m%d"))
            current_date += datetime.timedelta(days=1)

        # Erstelle ein Dictionary mit den Werten
        service_id_dicts_map[service_id] = {
            "start_date": start_date_string,
            "end_date": end_date_string,
            "deviations": deviations
        }

    del service_id_dates_map


    # Vorarbeit:

    # Hole die Information, welche Spalten in der Tabelle 'trips' vorhanden sind
    trips_columns = cache_db.execute(f"PRAGMA table_info(trips)").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings mit den Spaltennamen
    trips_columns = [column[1] for column in trips_columns]

    # Hole die Information, wo die Spalten 'service_id' und die Spalte 'record_id' in der Tabelle 'trips' vorhanden sind
    trips_service_id_index = trips_columns.index("service_id")
    trips_record_id_index = trips_columns.index("record_id")
    trips_trip_id_index = trips_columns.index("trip_id")

    # Baue ein Insert-Statement für die Tabelle 'trips' auf, um die neuen Einträge zu erstellen
    trip_insert_sql = f"INSERT INTO trips ({', '.join(trips_columns)}) VALUES ({', '.join(['?'] * len(trips_columns))})"


    # Hole die Information, welche Spalten in der Tabelle 'stop_times' vorhanden sind
    stop_times_columns = cache_db.execute(f"PRAGMA table_info(stop_times)").fetchall()
    # Konvertiere die Spalten in eine Liste von Strings mit den Spaltennamen
    stop_times_columns = [column[1] for column in stop_times_columns]

    # Hole die Information, wo die Spalten 'trip_id', 'stop_sequence' und 'record_id' in der Tabelle 'stop_times' vorhanden sind
    stop_times_trip_id_index = stop_times_columns.index("trip_id")
    stop_times_stop_sequence_index = stop_times_columns.index("stop_sequence")
    stop_times_record_id_index = stop_times_columns.index("record_id")

    # Baue ein Insert-Statement für die Tabelle 'stop_times' auf, um die neuen Einträge zu erstellen
    stop_times_insert_sql = f"INSERT INTO stop_times ({', '.join(stop_times_columns)}) VALUES ({', '.join(['?'] * len(stop_times_columns))})"


    # Lösche die alten Einträge in der Tabelle 'calendar_dates'
    delete_sql = f"DELETE FROM {old_table_name} WHERE exception_type = 1"
    cache_db.execute(delete_sql)
    cache_db.commit()

    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_trips_service_id ON trips (service_id);")
    cache_db.execute(f"CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id ON stop_times (trip_id);")
    cache_db.commit()

    
    # Füge die erarbeiteten Daten in die Tabellen ein

    for service_id, value in service_id_dicts_map.items():
        if stop_thread_var.get(): return

        new_service_id = service_id + "_positive_deviation"

        # Füge die Daten in die Tabelle 'calendar' ein
        insert_sql = f"INSERT INTO calendar (record_id, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        # Setze die Werte für die Wochentage auf 1 und füge die new_service_id als service_id ein und die start_date und end_date aus dem value als start_date und end_date
        values = (
            new_service_id,
            new_service_id,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            value["start_date"],
            value["end_date"]
        )

        # Führe das Insert-Statement aus
        cache_db.execute(insert_sql, values)
        cache_db.commit()



        # Füge die Daten der Abweichungen in die Tabelle 'calendar_dates' ein
        insert_sql = f"INSERT INTO calendar_dates (service_id, date, exception_type) VALUES (?, ?, ?)"

        # Generiere Tuples mit den Werten für die Abweichungen
        values = [(new_service_id, date, 2) for date in value["deviations"]]

        # Führe das Insert-Statement aus
        cache_db.executemany(insert_sql, values)
        cache_db.commit()



        # Dupliziere die Einträge in die Tabelle 'trips', bei denen die service_id die alte service_id ist
        # Sammle die record_ids der Einträge, die dupliziert werden sollen
        # Ersetze die record_ids für die neuen Einträge mit den record_ids + "_positive_deviation" der alten Einträge
        # und setze die service_id auf die neue service_id

        select_sql = f"SELECT * FROM trips WHERE service_id = ?"
        # Hole die record_ids der Einträge, die dupliziert werden sollen
        trips_values = cache_db.execute(select_sql, (service_id,)).fetchall()

        # Extrahiere die record_ids der Einträge, die dupliziert werden sollen
        trips_record_ids = [row[trips_record_id_index] for row in trips_values]

        # Ersetze in den trips_values die record_ids mit den neuen record_ids und setze die service_id auf die neue service_id
        new_trips_values = []
        for row in trips_values:
            new_row = list(row)
            new_row[trips_record_id_index] = row[trips_record_id_index] + "_positive_deviation"
            new_row[trips_trip_id_index] = row[trips_trip_id_index] + "_positive_deviation"
            new_row[trips_service_id_index] = new_service_id
            new_trips_values.append(tuple(new_row))
        # Füge die neuen Einträge in die Tabelle 'trips' ein
        cache_db.executemany(trip_insert_sql, new_trips_values)
        cache_db.commit()

        # Dupliziere die Einträge in die Tabelle 'stop_times', bei denen die trip_id die alte trip_id ist
        # Ersetze die trip_ids für die neuen Einträge mit den trip_ids + "_positive_deviation" der alten Einträge
        # Erstelle die record_ids der Einträge, die dupliziert werden sollen durch die neuen trip_ids + der alten stop_sequence

        for trips_record_id in trips_record_ids:
            # Hole die Einträge in den stop_times, die dupliziert werden sollen
            select_sql = f"SELECT * FROM stop_times WHERE trip_id = ?"
            stop_times_values = cache_db.execute(select_sql, (trips_record_id,)).fetchall()

            # Ersetze in den stop_times_values die record_ids mit den neuen record_ids und setze die trip_id auf die neue trip_id
            new_stop_times_values = []
            for row in stop_times_values:
                new_row = list(row)
                stop_times_sequence = row[stop_times_stop_sequence_index]
                new_row[stop_times_trip_id_index] = trips_record_id + "_positive_deviation"
                new_row[stop_times_record_id_index] = trips_record_id + "_positive_deviation" + str(stop_times_sequence)
                new_stop_times_values.append(tuple(new_row))
            # Füge die neuen Einträge in die Tabelle 'stop_times' ein
            cache_db.executemany(stop_times_insert_sql, new_stop_times_values)
            cache_db.commit()

    del service_id_dicts_map

    # Lösche die Indizes
    cache_db.execute(f"DROP INDEX IF EXISTS idx_calendar_dates_service_id_date;")
    cache_db.execute(f"DROP INDEX IF EXISTS idx_trips_service_id;")
    cache_db.execute(f"DROP INDEX IF EXISTS idx_stop_times_trip_id;")
    cache_db.commit()

    print(f"\r Positive Abweichungen erfolgreich aus der Tabelle {old_table_name} extrahiert")

