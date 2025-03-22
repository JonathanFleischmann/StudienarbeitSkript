from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_weekdays_database_table(calendar_gtfs_table):
    """
    Extrahiert die Wochentag-Kombinationen aus der GTFS-Tabelle 'calendar' und bildet sie auf die Datenbank-Tabelle 'weekdays' ab.
    
    :param calendar_gtfs_table: Die GTFS-Tabelle 'calendar'
    :return: Ein DataTable-Objekt für die Tabelle 'weekdays'
    """
    # Erhalte die Spalten der GTFS-Tabelle
    gtfs_table_columns = calendar_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle
    new_and_used_columns = append_new_columns_and_get_used("weekdays", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DataTable-Objekt für die Tabelle 'weekdays'
    weekdays_database_table = DataTable("weekdays", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle in die Datenbanktabelle ein
    weekdays_database_table.set_all_values(
        calendar_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    return weekdays_database_table