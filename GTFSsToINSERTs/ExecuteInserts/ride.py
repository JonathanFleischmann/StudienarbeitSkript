import sys
from data_storage import DataTable
from ExecuteInserts.datatype_enum import DatatypeEnum


def generate_ride_database_table_from_gtfs_tables(trips_gtfs_table, period_database_table, route_database_table, stop_times_gtfs_table):
    """
    Diese Funktion 
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    trips_gtfs_table_columns = trips_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle 

    # überprüfe, ob die notwendigen Spalten in der GTFS-Tabelle trips vorhanden sind    
    # direction_id_found = False
    necessary_values_found = {
        "route_id": False,
        "service_id": False
    }
    used_columns = []
    trip_headsign_found = False
    for column in trips_gtfs_table_columns:
        if column == "route_id":
            database_table_columns.append("route")
            necessary_values_found["route_id"] = True
            used_columns.append(column)
        elif column == "service_id":
            database_table_columns.append("period")
            necessary_values_found["service_id"] = True
            used_columns.append(column)
        # elif column == "direction_id":
            # direction_id_found = True
        elif column == "trip_headsign":
            database_table_columns.append("headsign")
            used_columns.append(column)
            trip_headsign_found = True
        elif column not in ["block_id", "shape_id", "wheelchair_accessible", "bikes_allowed", "trip_short_name", "direction_id"]:
            print(f"Die Spalte {column} wird nicht in der Datenbanktabelle route abgebildet.", file=sys.stderr)
            sys.exit(1)
    
    # Überprüfe
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle trips gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

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

    # überprüfe, ob die Spalten "stop_sequence", "trip_id" und "departure_time" in der GTFS-Tabelle stop_times vorhanden sind
    stop_times_gtfs_table_columns = stop_times_gtfs_table.get_columns()
    stop_times_headsign_found = False
    if "stop_sequence" not in stop_times_gtfs_table_columns:
        print("Die Spalte 'stop_sequence' wurde nicht in der GTFS-Tabelle stop_times gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
        sys.exit(1)
    if "departure_time" not in stop_times_gtfs_table_columns:
        print("Die Spalte 'departure_time' wurde nicht in der GTFS-Tabelle stop_times gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
        sys.exit(1)
    if "stop_headsign" in stop_times_gtfs_table_columns:
        stop_times_headsign_found = True

    # Füge die Spalte "departure_time" zur Datenbanktabelle hinzu
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