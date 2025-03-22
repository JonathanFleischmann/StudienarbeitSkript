from data_storage import DataTable, DatatypeEnum
from ExecuteInserts.core import append_new_columns_and_get_used


def generate_traffic_point_database_table_from_stops_gtfs_table(stops_gtfs_table, traffic_centre_database_table, location_type_database_table, height_database_table):
    """
    Diese Funktion 
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle stops vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = stops_gtfs_table.get_columns()

    new_and_used_columns = append_new_columns_and_get_used("traffic_point", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    traffic_centre_found = False
    if "parent_station" in gtfs_table_columns:
        traffic_centre_found = True
    height_found = False
    if "level_id" in gtfs_table_columns:
        height_found = True

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

    if traffic_centre_found:
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