from data_storage import DataTable

def generate_ride_exception_database_table(exception_table_database_table, ride_database_table, trips_gtfs_table, calendar_dates_gtfs_table):
    """
    Generiert die Datenbank-Tabelle 'ride_exception' aus den GTFS-Tabellen 'trips' und 'calendar_dates' sowie den Datenbank-Tabellen 'exception_table' und 'ride'.
    
    :param exception_table_database_table: Die Datenbank-Tabelle 'exception_table'
    :param ride_database_table: Die Datenbank-Tabelle 'ride'
    :param trips_gtfs_table: Die GTFS-Tabelle 'trips'
    :param calendar_dates_gtfs_table: Die GTFS-Tabelle 'calendar_dates'
    :return: Ein DataTable-Objekt für die Tabelle 'ride_exception'
    :raises KeyError: Wenn eine erforderliche Spalte nicht gefunden wird
    :raises ValueError: Wenn die Daten nicht korrekt verarbeitet werden können
    """
    # Erstelle ein DataTable-Objekt für die Tabelle 'ride_exception'
    ride_exception_database_table = DataTable("ride_exception", ["ride", "exception_table"])

    # Erhalte die eindeutigen Werte der Spalte 'service_id' aus der GTFS-Tabelle 'calendar_dates'
    service_date_id_service_id_map = calendar_dates_gtfs_table.get_distinct_values_of_all_records(["service_id"])
    service_date_id_exception_table_id_map = exception_table_database_table.get_distinct_values_of_all_records(["id"])
    trip_id_ride_id_map = ride_database_table.get_distinct_values_of_all_records(["id"])
    trip_id_service_id_map = trips_gtfs_table.get_distinct_values_of_all_records(["service_id"])

    # Erstelle eine Map von 'service_id' zu 'ride_ids'
    service_id_ride_ids_map = {}
    for trip_id, service_id in trip_id_service_id_map.items():
        if service_id[0] not in service_id_ride_ids_map:
            service_id_ride_ids_map[service_id[0]] = []
        ride_id = trip_id_ride_id_map[trip_id][0]
        if ride_id not in service_id_ride_ids_map[service_id[0]]:
            service_id_ride_ids_map[service_id[0]].append(ride_id)

    # Erstelle eine Map von 'service_id' zu 'exception_table_ids'
    service_id_exception_table_ids_map = {}
    for service_date_id, service_id in service_date_id_service_id_map.items():
        if service_id[0] not in service_id_exception_table_ids_map:
            service_id_exception_table_ids_map[service_id[0]] = []
        exception_table_id = service_date_id_exception_table_id_map[service_date_id][0]
        if exception_table_id not in service_id_exception_table_ids_map[service_id[0]]:
            service_id_exception_table_ids_map[service_id[0]].append(exception_table_id)

    exception_table_data = {}

    # Verknüpfe 'ride_ids' mit 'exception_table_ids' basierend auf 'service_id'
    for service_id, exception_table_ids in service_id_exception_table_ids_map.items():
        for ride_id in service_id_ride_ids_map[service_id]:
            for exception_table_id in exception_table_ids:
                ride_exception_id = ride_id + exception_table_id
                exception_table_data[ride_exception_id] = [ride_id, exception_table_id]

    # Setze alle Werte in die Datenbanktabelle 'ride_exception'
    ride_exception_database_table.set_all_values(exception_table_data)

    return ride_exception_database_table