import sys
from ExecuteInserts.data_storage import DatabaseTable
from ExecuteInserts.core import map_to_oracle_date


def generate_period_database_table_from_gtfs_table(calendar_gtfs_table, weekdays_database_table):
    """
    Diese Funktion extrahiert die Start- und Enddaten aus der GTFS-Tabelle calendar
    und bildet sie auf die Datenbank-Tabelle period ab. Außerdem wird die Referenz auf die Tabelle weekdays gesetzt.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = calendar_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    necessary_values_found = {
        "start_date": False,
        "end_date": False
    }
    for column in gtfs_table_columns:
        if column in necessary_values_found:
            database_table_columns.append(column)
            necessary_values_found[column] = True
        elif column not in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
            print(f"Die Spalte {column} wird nicht in der Datenbanktabelle route abgebildet.", file=sys.stderr)
            sys.exit(1)
    
    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle routes gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    period_database_table = DatabaseTable("period", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    period_database_table.set_all_values(
        calendar_gtfs_table.get_distinct_attributes_of_all_records(["start_date", "end_date"])
    )

    # Füge die Spalte für die Foreign-Keys auf die weekdays hinzu
    period_database_table.add_column("weekdays")

    # Füge die Werte der Foreign-Keys auf die weekdays hinzu
    for record_id, record in weekdays_database_table.get_distinct_attributes_of_all_records(["id"]).items():
        period_database_table.set_value(record_id, "weekdays", record[0])

    period_database_table.add_unique_columns(["start_date", "end_date", "weekdays"])

    period_database_table.set_data_types(
        {
            "start_date": "DATE",
            "end_date": "DATE",
            "weekdays": "INTEGER"
        }
    )

    # ersetze die Date-Werte durch die mit Oracle kompatiblen Werte mithilfe der Methode map_to_oracle_date aus core.py
    period_database_table.apply_map_function_to_column("start_date", map_to_oracle_date)
    period_database_table.apply_map_function_to_column("end_date", map_to_oracle_date)

    return period_database_table