import sys
from ExecuteInserts.data_storage import DatabaseTable


def generate_ride_database_table_from_gtfs_tables(trips_gtfs_table, period_database_table, route_database_table):
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
        elif column not in ["block_id", "shape_id", "wheelchair_accessible", "bikes_allowed", "trip_short_name", "direction_id"]:
            print(f"Die Spalte {column} wird nicht in der Datenbanktabelle route abgebildet.", file=sys.stderr)
            sys.exit(1)
    
    # Überprüfe
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle trips gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    ride_database_table = DatabaseTable("ride", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    ride_database_table.set_all_values(
        trips_gtfs_table.get_distinct_attributes_of_all_records(used_columns)
    )

    ride_database_table.add_unique_columns(["route", "period", "headsign"])

    ride_database_table.set_data_types(
        {
            "route": "INTEGER",
            "period": "INTEGER",
            "headsign": "TEXT"
        }
    )

    # ersetze die route_id aus dem gtfs-file durch die neu generierte id der route-table
    route_id_map = route_database_table.get_distinct_attributes_of_all_records(["id"])
    for record_id, route in ride_database_table.get_distinct_attributes_of_all_records(["route"]).items():
        route_new_id = route_id_map[route[0]][0]
        ride_database_table.set_value(record_id, "route", route_new_id)

    # ersetze die service_id aus dem gtfs-file durch die neu generierte id der period-table
    period_id_map = period_database_table.get_distinct_attributes_of_all_records(["id"])
    for record_id, period in ride_database_table.get_distinct_attributes_of_all_records(["period"]).items():
        period_new_id = period_id_map[period[0]][0]
        ride_database_table.set_value(record_id, "period", period_new_id)


    return ride_database_table