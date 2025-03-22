from data_storage import DataTable, DatatypeEnum
from ExecuteInserts.core import append_new_columns_and_get_used


def generate_route_database_table_from_gtfs_table(routes_gtfs_table, agency_database_table):
    """
    Diese Funktion bildet die GTFS-Tabelle routes auf die Datenbank-Tabelle route ab mithilfe der Foreign-Reference auf die agency-Tabelle.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = routes_gtfs_table.get_columns()
    
    new_and_used_columns = append_new_columns_and_get_used("route", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    route_database_table = DataTable("route", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    route_database_table.set_all_values(
        routes_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )
    
    # ersetze die agency_id aus dem gtfs-file durch die neu generierte id der agency
    agency_id_map = agency_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, agency in route_database_table.get_distinct_values_of_all_records(["agency"]).items():
        agency_new_id = agency_id_map[agency[0]][0]
        route_database_table.set_value(record_id, "agency", agency_new_id)

    return route_database_table