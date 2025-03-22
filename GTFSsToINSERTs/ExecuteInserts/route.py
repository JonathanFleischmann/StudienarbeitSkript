from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_route_database_table_from_gtfs_table(routes_gtfs_table, agency_database_table):
    """
    Generiert die Datenbank-Tabelle 'route' aus der GTFS-Tabelle 'routes' und setzt die Foreign-Reference auf die Tabelle 'agency'.
    
    :param routes_gtfs_table: Die GTFS-Tabelle 'routes'
    :param agency_database_table: Die Datenbank-Tabelle 'agency'
    :return: Ein DataTable-Objekt für die Tabelle 'route'
    :raises KeyError: Wenn eine erforderliche Spalte nicht gefunden wird
    :raises ValueError: Wenn die Daten nicht korrekt verarbeitet werden können
    """
    # Erhalte die Spalten der GTFS-Tabelle 'routes'
    gtfs_table_columns = routes_gtfs_table.get_columns()
    
    # Bestimme neue und verwendete Spalten für die Datenbanktabelle 'route'
    new_and_used_columns = append_new_columns_and_get_used("route", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DataTable-Objekt für die Tabelle 'route'
    route_database_table = DataTable("route", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle 'routes' in die Datenbanktabelle ein
    route_database_table.set_all_values(
        routes_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )
    
    # Ersetze die 'agency_id' aus dem GTFS-File durch die neu generierte ID der 'agency'-Tabelle
    agency_id_map = agency_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, agency in route_database_table.get_distinct_values_of_all_records(["agency"]).items():
        agency_new_id = agency_id_map[agency[0]][0]
        route_database_table.set_value(record_id, "agency", agency_new_id)

    return route_database_table