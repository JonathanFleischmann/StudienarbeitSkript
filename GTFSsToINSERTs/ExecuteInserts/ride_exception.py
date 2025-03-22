from data_storage import DataTable, DatatypeEnum


def generate_ride_exception_database_table_from_gtfs_tables(exception_table_database_table, ride_database_table, trips_gtfs_table, calendar_dates_gtfs_table):
    """
    Diese Funktion 
    """

    ride_exception_database_table = DataTable("ride_exception", ["ride", "exception_table"])

    service_date_id_service_id_map = calendar_dates_gtfs_table.get_distinct_values_of_all_records(["service_id"])
    service_date_id_exception_table_id_map = exception_table_database_table.get_distinct_values_of_all_records(["id"])
    trip_id_ride_id_map = ride_database_table.get_distinct_values_of_all_records(["id"])
    trip_id_service_id_map = trips_gtfs_table.get_distinct_values_of_all_records(["service_id"])

    service_id_ride_ids_map = {}
    for trip_id, service_id in trip_id_service_id_map.items():
        if service_id[0] not in service_id_ride_ids_map:
            service_id_ride_ids_map[service_id[0]] = []
        ride_id = trip_id_ride_id_map[trip_id][0]
        if ride_id not in service_id_ride_ids_map[service_id[0]]:
            service_id_ride_ids_map[service_id[0]].append(ride_id)

    service_id_exception_table_ids_map = {}
    for service_date_id, service_id in service_date_id_service_id_map.items():
        if service_id[0] not in service_id_exception_table_ids_map:
            service_id_exception_table_ids_map[service_id[0]] = []
        exception_table_id = service_date_id_exception_table_id_map[service_date_id][0]
        if exception_table_id not in service_id_exception_table_ids_map[service_id[0]]:
            service_id_exception_table_ids_map[service_id[0]].append(exception_table_id)

    exception_table_data = {}

    for service_id, exception_table_ids in service_id_exception_table_ids_map.items():
        for ride_id in service_id_ride_ids_map[service_id]:
            for exception_table_id in exception_table_ids:
                ride_exception_id = ride_id + exception_table_id
                exception_table_data[ride_exception_id] = [ride_id, exception_table_id]

    ride_exception_database_table.set_all_values(exception_table_data)

    ride_exception_database_table.add_unique_column("ride")
    ride_exception_database_table.add_unique_column("exception_table")

    ride_exception_database_table.set_data_types(
        {
            "ride": DatatypeEnum.INTEGER,
            "exception_table": DatatypeEnum.INTEGER
        }
    )

    return ride_exception_database_table