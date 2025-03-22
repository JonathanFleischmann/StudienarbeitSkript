from data_storage import DataTable
from ExecuteInserts.core import append_new_columns_and_get_used

def generate_period_database_table(calendar_gtfs_table, weekdays_database_table):
    """
    Generiert die Datenbank-Tabelle 'period' aus der GTFS-Tabelle 'calendar' und setzt die Referenz auf die Tabelle 'weekdays'.
    
    :param calendar_gtfs_table: Die GTFS-Tabelle 'calendar'
    :param weekdays_database_table: Die Datenbank-Tabelle 'weekdays'
    :return: Ein DataTable-Objekt für die Tabelle 'period'
    :raises KeyError: Wenn eine erforderliche Spalte nicht gefunden wird
    :raises ValueError: Wenn die Daten nicht korrekt verarbeitet werden können
    """
    # Erhalte die Spalten der GTFS-Tabelle 'calendar'
    gtfs_table_columns = calendar_gtfs_table.get_columns()

    # Bestimme neue und verwendete Spalten für die Datenbanktabelle 'period'
    new_and_used_columns = append_new_columns_and_get_used("period", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DataTable-Objekt für die Tabelle 'period'
    period_database_table = DataTable("period", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle in die Datenbanktabelle ein
    period_database_table.set_all_values(
        calendar_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    # Füge die Spalte für die Foreign-Keys auf die 'weekdays' hinzu
    period_database_table.add_column("weekdays")

    # Setze die Werte der Foreign-Keys auf die 'weekdays'
    for record_id, record in weekdays_database_table.get_distinct_values_of_all_records(["id"]).items():
        period_database_table.set_value(record_id, "weekdays", record[0])

    return period_database_table