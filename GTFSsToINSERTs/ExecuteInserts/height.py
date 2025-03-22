from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_height_database_table_from_gtfs_table(levels_gtfs_table):
    """
    Bildet die GTFS-Tabelle 'levels' auf die Datenbank-Tabelle 'height' ab.
    
    :param levels_gtfs_table: Die GTFS-Tabelle 'levels'
    :return: Ein DataTable-Objekt für die Tabelle 'height'
    """
    # Erhalte die Spalten der GTFS-Tabelle
    gtfs_table_columns = levels_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle
    new_and_used_columns = append_new_columns_and_get_used("height", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DataTable-Objekt für die Tabelle 'height'
    height_database_table = DataTable("height", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle in die Datenbanktabelle ein
    height_database_table.set_all_values(
        levels_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    return height_database_table