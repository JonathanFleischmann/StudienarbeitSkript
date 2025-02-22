import sys
from ExecuteInserts.data_storage import DatabaseTable
from ExecuteInserts.datatype_enum import DatatypeEnum


def generate_traffic_centre_database_table_from_gtfs_tables_and_remove_centres_from_stops(stops_gtfs_table, stop_times_gtfs_table, location_type_database_table):
    """
    Diese Funktion 
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle stops vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = stops_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    necessary_values_found = {
        "stop_name": False,
        "stop_lat": False,
        "stop_lon": False,
        "location_type": False,
        "parent_station": False
    }
    used_columns = []
    for column in gtfs_table_columns:
        if column == "stop_name":
            database_table_columns.append("name")
            necessary_values_found["stop_name"] = True
            used_columns.append(column)
        elif column == "stop_lat":
            database_table_columns.append("latitude")
            necessary_values_found["stop_lat"] = True
            used_columns.append(column)
        elif column == "stop_lon":
            database_table_columns.append("longitude")
            necessary_values_found["stop_lon"] = True
            used_columns.append(column)
        elif column == "location_type":
            database_table_columns.append("location_type")
            necessary_values_found["location_type"] = True
            used_columns.append(column)
        elif column == "parent_station":
            necessary_values_found["parent_station"] = True
        elif column not in ["stop_code", "stop_desc", "platform_code", "wheelchair_boarding", "level_id"]:
            print(f"Die Spalte {column} wurde nicht für die GTFS-Tabelle stops erwartet", file=sys.stderr)
            sys.exit(1)
    


    if not necessary_values_found["parent_station"]:
        print(f"Es wurden keine Verknüpfungen zu Verkehrsknotenpunkten gefunden", file=sys.stderr)
        
        traffic_centre_database_table = DatabaseTable("traffic_centre", database_table_columns)
        
        traffic_centre_database_table.add_unique_column("name")
        traffic_centre_database_table.add_unique_column("latitude")
        traffic_centre_database_table.add_unique_column("longitude")
        traffic_centre_database_table.add_unique_column("location_type")

        traffic_centre_database_table.set_data_types(
            {
                "name": DatatypeEnum.TEXT,
                "latitude": DatatypeEnum.FLOAT,
                "longitude": DatatypeEnum.FLOAT,
                "location_type": DatatypeEnum.INTEGER
            }
        )

        return traffic_centre_database_table



    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle stops gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle 
    traffic_centre_database_table = DatabaseTable("traffic_centre", database_table_columns)

    traffic_centre_database_table.add_unique_column("name")
    traffic_centre_database_table.add_unique_column("latitude")
    traffic_centre_database_table.add_unique_column("longitude")
    traffic_centre_database_table.add_unique_column("location_type")

    traffic_centre_database_table.set_data_types(
        {
            "name": DatatypeEnum.TEXT,
            "latitude": DatatypeEnum.FLOAT,
            "longitude": DatatypeEnum.FLOAT,
            "location_type": DatatypeEnum.INTEGER
        }
    )

    stops = stops_gtfs_table.get_distinct_attributes_of_all_records(used_columns)

    # Finde in der GTFS-Tabelle stops alle Einträge, die parent_stations von anderen Einträgen sind,
    # selbst aber keine parent_station haben und auf die keine Referenz von stop_times existiert
    parent_stations = stops_gtfs_table.get_map_with_column_as_key_and_id_as_value("parent_station")
    stop_times = stop_times_gtfs_table.get_map_with_column_as_key_and_id_as_value("stop_id")
    location_type_index = stops_gtfs_table.get_columns().index("location_type")
    for parent_station, stop_ids in parent_stations.items():
        if parent_station is None:
            continue
        if (stops_gtfs_table.get_attribute(parent_station, "parent_station") is None or stops_gtfs_table.get_attribute(parent_station, "parent_station") == "") and parent_station not in stop_times:
            traffic_centre_database_table.add_record(parent_station, stops[parent_station])
            stops_gtfs_table.delete_record(parent_station)

    # ersetze den type aus dem gtfs-file durch die neu generierte id des location_type
    location_type_id_map = location_type_database_table.get_distinct_attributes_of_all_records(["id"])
    for record_id, type in traffic_centre_database_table.get_distinct_attributes_of_all_records(["location_type"]).items():
        location_type_new_id = location_type_id_map[type[0]][0]
        traffic_centre_database_table.set_value(record_id, "location_type", location_type_new_id)

    return traffic_centre_database_table