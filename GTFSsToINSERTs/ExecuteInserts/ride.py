from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_ride_database_table_from_gtfs_tables(trips_gtfs_table, period_database_table, route_database_table, stop_times_gtfs_table):
    """
    Generiert die Datenbank-Tabelle 'ride' aus den GTFS-Tabellen 'trips' und 'stop_times' sowie den Datenbank-Tabellen 'period' und 'route'.
    
    :param trips_gtfs_table: Die GTFS-Tabelle 'trips'
    :param period_database_table: Die Datenbank-Tabelle 'period'
    :param route_database_table: Die Datenbank-Tabelle 'route'
    :param stop_times_gtfs_table: Die GTFS-Tabelle 'stop_times'
    :return: Ein DataTable-Objekt für die Tabelle 'ride'
    :raises KeyError: Wenn eine erforderliche Spalte nicht gefunden wird
    :raises ValueError: Wenn die Daten nicht korrekt verarbeitet werden können
    """
    # Erhalte die Spalten der GTFS-Tabellen 'trips' und 'stop_times'
    trips_gtfs_table_columns = trips_gtfs_table.get_columns()
    stop_times_gtfs_table_columns = stop_times_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle 'ride'
    new_and_used_columns = append_new_columns_and_get_used("ride", trips_gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Überprüfe, ob die Spalten 'trip_headsign' und 'stop_headsign' vorhanden sind
    trip_headsign_found = "trip_headsign" in trips_gtfs_table_columns
    stop_times_headsign_found = "stop_headsign" in stop_times_gtfs_table_columns

    # Erstelle ein DataTable-Objekt für die Tabelle 'ride'
    ride_database_table = DataTable("ride", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle 'trips' in die Datenbanktabelle ein
    ride_database_table.set_all_values(
        trips_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    # Ersetze die 'route_id' aus dem GTFS-File durch die neu generierte ID der 'route'-Tabelle
    route_id_map = route_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, route in ride_database_table.get_distinct_values_of_all_records(["route"]).items():
        route_new_id = route_id_map[route[0]][0]
        ride_database_table.set_value(record_id, "route", route_new_id)

    # Ersetze die 'service_id' aus dem GTFS-File durch die neu generierte ID der 'period'-Tabelle
    period_id_map = period_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, period in ride_database_table.get_distinct_values_of_all_records(["period"]).items():
        period_new_id = period_id_map[period[0]][0]
        ride_database_table.set_value(record_id, "period", period_new_id)

    # Füge die Spalte 'start_time' zur Datenbanktabelle hinzu
    ride_database_table.add_column("start_time")

    # Weise jedem Ride die Startzeit zu
    for trip_id, record in ride_database_table.get_all_records().items():
        start_stop_time = trip_id + "1"
        start_time = stop_times_gtfs_table.get_value(start_stop_time, "departure_time")
        ride_database_table.set_value(trip_id, "start_time", start_time)

    # Weise jedem Ride den 'headsign' zu oder ersetze den 'headsign' aus dem GTFS-File, wenn für die Spalte kein Wert vorhanden ist
    if stop_times_headsign_found:
        if not trip_headsign_found:
            ride_database_table.add_column("headsign")
        else:
            headsign_index = ride_database_table.get_columns().index("headsign")
        for trip_id, record in ride_database_table.get_all_records().items():
            if trip_headsign_found and record[headsign_index] != "" and record[headsign_index] is not None:
                continue
            start_stop_time = trip_id + "1"
            headsign = stop_times_gtfs_table.get_value(start_stop_time, "stop_headsign")
            ride_database_table.set_value(trip_id, "headsign", headsign)

    return ride_database_table