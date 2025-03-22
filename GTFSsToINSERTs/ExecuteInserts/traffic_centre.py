import sys
from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_traffic_centre_database_table_from_gtfs_tables_and_remove_centres_from_stops(stops_gtfs_table, stop_times_gtfs_table, location_type_database_table):
    """
    Generiert die Datenbank-Tabelle 'traffic_centre' aus den GTFS-Tabellen 'stops' und 'stop_times' und entfernt Zentren aus den Haltestellen.
    
    :param stops_gtfs_table: Die GTFS-Tabelle 'stops'
    :param stop_times_gtfs_table: Die GTFS-Tabelle 'stop_times'
    :param location_type_database_table: Die Datenbank-Tabelle 'location_type'
    :return: Ein DataTable-Objekt für die Tabelle 'traffic_centre'
    :raises KeyError: Wenn eine erforderliche Spalte nicht gefunden wird
    :raises ValueError: Wenn die Daten nicht korrekt verarbeitet werden können
    """
    # Erhalte die Spalten der GTFS-Tabelle 'stops'
    gtfs_table_columns = stops_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle 'traffic_centre'
    new_and_used_columns = append_new_columns_and_get_used("traffic_centre", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Überprüfe, ob die Spalte 'parent_station' vorhanden ist
    if "parent_station" not in gtfs_table_columns:
        print(f"Es wurden keine Verknüpfungen zu zentralen Verkehrsknotenpunkten gefunden", file=sys.stderr)
        
        # Erstelle ein DataTable-Objekt für die Tabelle 'traffic_centre'
        traffic_centre_database_table = DataTable("traffic_centre", database_table_columns)

        return traffic_centre_database_table

    # Erstelle ein DataTable-Objekt für die Tabelle 'traffic_centre'
    traffic_centre_database_table = DataTable("traffic_centre", database_table_columns)

    # Erhalte die eindeutigen Werte der GTFS-Tabelle 'stops'
    stops = stops_gtfs_table.get_distinct_values_of_all_records(used_columns)

    # Finde in der GTFS-Tabelle 'stops' alle Einträge, die parent_stations von anderen Einträgen sind,
    # selbst aber keine parent_station haben und auf die keine Referenz von 'stop_times' existiert
    parent_stations = stops_gtfs_table.get_map_with_column_as_key_and_id_as_value("parent_station")
    stop_times = stop_times_gtfs_table.get_map_with_column_as_key_and_id_as_value("stop_id")
    location_type_index = stops_gtfs_table.get_columns().index("location_type")
    for parent_station, stop_ids in parent_stations.items():
        if parent_station is None:
            continue
        if (stops_gtfs_table.get_value(parent_station, "parent_station") is None or stops_gtfs_table.get_value(parent_station, "parent_station") == "") and parent_station not in stop_times:
            traffic_centre_database_table.add_record(parent_station, stops[parent_station])
            stops_gtfs_table.delete_record(parent_station)

    # Ersetze den Typ aus dem GTFS-File durch die neu generierte ID des 'location_type'
    location_type_id_map = location_type_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, type in traffic_centre_database_table.get_distinct_values_of_all_records(["location_type"]).items():
        location_type_new_id = location_type_id_map[type[0]][0]
        traffic_centre_database_table.set_value(record_id, "location_type", location_type_new_id)

    return traffic_centre_database_table