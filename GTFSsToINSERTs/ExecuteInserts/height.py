from data_storage import DataTable, DatatypeEnum
from ExecuteInserts.core import append_new_columns_and_get_used


def generate_height_database_table_from_gtfs_table(levels_gtfs_table):
    """
    Diese Funktion bildet die GTFS-Tabelle agency auf die Datenbank-Tabelle agency ab.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle agency vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = levels_gtfs_table.get_columns()

    new_and_used_columns = append_new_columns_and_get_used("height", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DatabaseTable-Objekt für die Tabelle agency
    height_database_table = DataTable("height", database_table_columns)

    height_database_table.add_unique_column("name")
    height_database_table.add_unique_column("above_sea_level")
    height_database_table.add_unique_column("floor")

    # Füge die Datensätze der GTFS-Tabelle agency in die Datenbanktabelle ein
    height_database_table.set_all_values(
        levels_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    return height_database_table