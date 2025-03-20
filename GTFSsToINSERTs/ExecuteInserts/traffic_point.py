import sys
from data_storage import DataTable
from ExecuteInserts.datatype_enum import DatatypeEnum


def generate_traffic_point_database_table_from_stops_gtfs_table(stops_gtfs_table, traffic_centre_database_table, location_type_database_table, height_database_table):
    """
    Diese Funktion 
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle stops vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = stops_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    parent_station_found = False
    height_found = False
    necessary_values_found = {
        "stop_lat": False,
        "stop_lon": False,
        "location_type": False
    }
    used_columns = []
    for column in gtfs_table_columns:
        if column == "stop_name":
            database_table_columns.append("name")
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
            parent_station_found = True
            database_table_columns.append("traffic_centre")
            used_columns.append(column)
        elif column == "level_id":
            height_found = True
            database_table_columns.append("height")
            used_columns.append(column)
        elif column not in ["stop_code", "stop_desc", "platform_code", "wheelchair_boarding", "level_id"]:
            print(f"Die Spalte {column} wurde nicht für die GTFS-Tabelle stops erwartet", file=sys.stderr)
            sys.exit(1)
    

    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle stops gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle 
    traffic_point_database_table = DataTable("traffic_point", database_table_columns)

    traffic_point_database_table.add_unique_column("latitude")
    traffic_point_database_table.add_unique_column("longitude")

    traffic_point_database_table.set_data_types(
        {
            "name": DatatypeEnum.TEXT,
            "latitude": DatatypeEnum.FLOAT,
            "longitude": DatatypeEnum.FLOAT,
            "location_type": DatatypeEnum.INTEGER,
            "traffic_centre": DatatypeEnum.INTEGER,
            "height": DatatypeEnum.INTEGER
        }
    )

    # Füge die Datensätze der GTFS-Tabelle stops in die Datenbanktabelle ein
    traffic_point_database_table.set_all_values(
        stops_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    if parent_station_found:
        traffic_centre_id_map = traffic_centre_database_table.get_distinct_values_of_all_records(["id"])
        for record_id, traffic_centre_id in traffic_point_database_table.get_distinct_values_of_all_records(["traffic_centre"]).items():
            if traffic_centre_id[0] == "":
                continue
            traffic_centre_new_id = traffic_centre_id_map[traffic_centre_id[0]][0]
            traffic_point_database_table.set_value(record_id, "traffic_centre", traffic_centre_new_id)

    if height_found:
        height_id_map = height_database_table.get_distinct_values_of_all_records(["id"])
        for record_id, height_id in traffic_point_database_table.get_distinct_values_of_all_records(["height"]).items():
            if height_id[0] == "":
                continue
            height_new_id = height_id_map[height_id[0]][0]
            traffic_point_database_table.set_value(record_id, "height", height_new_id)


    # ersetze den type aus dem gtfs-file durch die neu generierte id des location_type
    location_type_id_map = location_type_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, type in traffic_centre_database_table.get_distinct_values_of_all_records(["location_type"]).items():
        if type[0] == "":
            continue
        location_type_new_id = location_type_id_map[str(type[0])][0]
        traffic_centre_database_table.set_value(record_id, "location_type", location_type_new_id)

    return traffic_point_database_table