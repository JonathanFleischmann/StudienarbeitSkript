from data_storage import DataTable, DatatypeEnum
from ExecuteInserts.core import append_new_columns_and_get_used


def generate_period_database_table_from_gtfs_table(calendar_gtfs_table, weekdays_database_table):
    """
    Diese Funktion extrahiert die Start- und Enddaten aus der GTFS-Tabelle calendar
    und bildet sie auf die Datenbank-Tabelle period ab. Außerdem wird die Referenz auf die Tabelle weekdays gesetzt.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = calendar_gtfs_table.get_columns()

    new_and_used_columns = append_new_columns_and_get_used("period", gtfs_table_columns)

    database_table_columns = new_and_used_columns["new_columns"]
    used_columns = new_and_used_columns["used_columns"]

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    period_database_table = DataTable("period", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    period_database_table.set_all_values(
        calendar_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    # Füge die Spalte für die Foreign-Keys auf die weekdays hinzu
    period_database_table.add_column("weekdays")

    # Füge die Werte der Foreign-Keys auf die weekdays hinzu
    for record_id, record in weekdays_database_table.get_distinct_values_of_all_records(["id"]).items():
        period_database_table.set_value(record_id, "weekdays", record[0])

    period_database_table.add_unique_column("start_date")
    period_database_table.add_unique_column("end_date")
    period_database_table.add_unique_column("weekdays")

    period_database_table.set_data_types(
        {
            "start_date": DatatypeEnum.DATE,
            "end_date": DatatypeEnum.DATE,
            "weekdays": DatatypeEnum.INTEGER
        }
    )

    # ersetze die Date-Werte durch die mit Oracle kompatiblen Werte mithilfe der Methode map_to_oracle_date aus core.py
    # period_database_table.apply_map_function_to_column("start_date", map_to_oracle_date)
    # period_database_table.apply_map_function_to_column("end_date", map_to_oracle_date)

    return period_database_table