from data_storage import DataTable, DatatypeEnum
from ExecuteInserts.core import append_new_columns_and_get_used


def generate_ride_database_table_from_gtfs_tables(trips_gtfs_table, period_database_table, route_database_table, stop_times_gtfs_table):
    """
    Diese Funktion 
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    trips_gtfs_table_columns = trips_gtfs_table.get_columns()
    stop_times_gtfs_table_columns = stop_times_gtfs_table.get_columns()

    new_and_used_columns = append_new_columns_and_get_used("ride", trips_gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]
    # TODO: direction_id?

    trip_headsign_found = False
    if "trip_headsign" in trips_gtfs_table_columns:
        trip_headsign_found = True

    stop_times_headsign_found = False
    if "stop_headsign" in stop_times_gtfs_table_columns:
        stop_times_headsign_found = True

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    ride_database_table = DataTable("ride", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    ride_database_table.set_all_values(
        trips_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    ride_database_table.add_unique_column("route")
    ride_database_table.add_unique_column("period")
    ride_database_table.add_unique_column("start_time")

    ride_database_table.set_data_types(
        {
            "route": DatatypeEnum.INTEGER,
            "period": DatatypeEnum.INTEGER,
            "headsign": DatatypeEnum.TEXT,
            "start_time": DatatypeEnum.TIME
        }
    )

    # ersetze die route_id aus dem gtfs-file durch die neu generierte id der route-table
    route_id_map = route_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, route in ride_database_table.get_distinct_values_of_all_records(["route"]).items():
        route_new_id = route_id_map[route[0]][0]
        ride_database_table.set_value(record_id, "route", route_new_id)

    # ersetze die service_id aus dem gtfs-file durch die neu generierte id der period-table
    period_id_map = period_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, period in ride_database_table.get_distinct_values_of_all_records(["period"]).items():
        period_new_id = period_id_map[period[0]][0]
        ride_database_table.set_value(record_id, "period", period_new_id)

    # Füge die Spalte "start_time" zur Datenbanktabelle hinzu
    ride_database_table.add_column("start_time")

    # Weise jedem Ride die Startzeit hinzu
    for trip_id, record in ride_database_table.get_all_records().items():
        start_stop_time = trip_id + "1"
        start_time = stop_times_gtfs_table.get_value(start_stop_time, "departure_time")
        ride_database_table.set_value(trip_id, "start_time", start_time)

    if stop_times_headsign_found:
        if not trip_headsign_found:
            ride_database_table.add_column("headsign")
        else:
            headsign_index = ride_database_table.get_columns().index("headsign")
        # Weise jedem Ride den headsign hinzu oder ersetze den headsign aus dem GTFS-File, wenn für die Spalte kein Wert vorhanden ist
        for trip_id, record in ride_database_table.get_all_records().items():
            if trip_headsign_found and record[headsign_index] != "" and record[headsign_index] != None:
                continue
            start_stop_time = trip_id + "1"
            headsign = stop_times_gtfs_table.get_value(start_stop_time, "stop_headsign")
            ride_database_table.set_value(trip_id, "headsign", headsign)

    return ride_database_table