from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used


def generate_agency_database_table_from_gtfs_table(agency_gtfs_table):
    """
    Diese Funktion bildet die GTFS-Tabelle agency auf die Datenbank-Tabelle agency ab.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle agency vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = agency_gtfs_table.get_columns()

    new_and_used_columns = append_new_columns_and_get_used("agency", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DatabaseTable-Objekt für die Tabelle agency
    agency_database_table = DataTable("agency", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle agency in die Datenbanktabelle ein
    agency_database_table.set_all_values(
        agency_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    return agency_database_table