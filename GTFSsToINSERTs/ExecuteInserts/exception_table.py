from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_exception_table_database_table_from_gtfs_table(calendar_dates_gtfs_table):
    """
    Bildet die GTFS-Tabelle 'calendar_dates' auf die Datenbank-Tabelle 'exception_table' ab.
    
    :param calendar_dates_gtfs_table: Die GTFS-Tabelle 'calendar_dates'
    :return: Ein DataTable-Objekt für die Tabelle 'exception_table'
    """
    # Erhalte die Spalten der GTFS-Tabelle
    gtfs_table_columns = calendar_dates_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle
    new_and_used_columns = append_new_columns_and_get_used("exception_table", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DataTable-Objekt für die Tabelle 'exception_table'
    exception_table_database_table = DataTable("exception_table", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle in die Datenbanktabelle ein
    exception_table_database_table.set_all_values(
        calendar_dates_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    return exception_table_database_table