from data_storage import DataTable 
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_traffic_point_database_table_from_stops_gtfs_table(stops_gtfs_table, traffic_centre_database_table, location_type_database_table, height_database_table):
    """
    Generiert die Datenbank-Tabelle 'traffic_point' aus der GTFS-Tabelle 'stops' und setzt die Referenzen auf die Tabellen 'traffic_centre', 'location_type' und 'height'.
    
    :param stops_gtfs_table: Die GTFS-Tabelle 'stops'
    :param traffic_centre_database_table: Die Datenbank-Tabelle 'traffic_centre'
    :param location_type_database_table: Die Datenbank-Tabelle 'location_type'
    :param height_database_table: Die Datenbank-Tabelle 'height'
    :return: Ein DataTable-Objekt für die Tabelle 'traffic_point'
    :raises KeyError: Wenn eine erforderliche Spalte nicht gefunden wird
    :raises ValueError: Wenn die Daten nicht korrekt verarbeitet werden können
    """
    # Erhalte die Spalten der GTFS-Tabelle 'stops'
    gtfs_table_columns = stops_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle 'traffic_point'
    new_and_used_columns = append_new_columns_and_get_used("traffic_point", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Überprüfe, ob die Spalten 'parent_station' und 'level_id' vorhanden sind
    traffic_centre_found = "parent_station" in gtfs_table_columns
    height_found = "level_id" in gtfs_table_columns

    # Erstelle ein DataTable-Objekt für die Tabelle 'traffic_point'
    traffic_point_database_table = DataTable("traffic_point", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle 'stops' in die Datenbanktabelle ein
    traffic_point_database_table.set_all_values(
        stops_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    # Setze die Referenzen auf die Tabelle 'traffic_centre'
    if traffic_centre_found:
        traffic_centre_id_map = traffic_centre_database_table.get_distinct_values_of_all_records(["id"])
        for record_id, traffic_centre_id in traffic_point_database_table.get_distinct_values_of_all_records(["traffic_centre"]).items():
            if traffic_centre_id[0] == "":
                continue
            traffic_centre_new_id = traffic_centre_id_map[traffic_centre_id[0]][0]
            traffic_point_database_table.set_value(record_id, "traffic_centre", traffic_centre_new_id)

    # Setze die Referenzen auf die Tabelle 'height'
    if height_found:
        height_id_map = height_database_table.get_distinct_values_of_all_records(["id"])
        for record_id, height_id in traffic_point_database_table.get_distinct_values_of_all_records(["height"]).items():
            if height_id[0] == "":
                continue
            height_new_id = height_id_map[height_id[0]][0]
            traffic_point_database_table.set_value(record_id, "height", height_new_id)

    # Ersetze den Typ aus dem GTFS-File durch die neu generierte ID des 'location_type'
    location_type_id_map = location_type_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, type in traffic_centre_database_table.get_distinct_values_of_all_records(["location_type"]).items():
        if type[0] == "":
            continue
        location_type_new_id = location_type_id_map[str(type[0])][0]
        traffic_centre_database_table.set_value(record_id, "location_type", location_type_new_id)

    return traffic_point_database_table