import sys
from data_storage import DataTable
from ExecuteInserts.datatype_enum import DatatypeEnum


def generate_exception_table_database_table_from_gtfs_table(calendar_dates_gtfs_table):
    """
    Diese Funktion 
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = calendar_dates_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    necessary_values_found = {
        "date": False
    }
    used_columns = []
    for column in gtfs_table_columns:
        if column == "date":
            database_table_columns.append("date_col")
            necessary_values_found[column] = True
            used_columns.append(column)
    
    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle routes gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    exception_table_database_table = DataTable("exception_table", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    exception_table_database_table.set_all_values(
        calendar_dates_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    exception_table_database_table.add_unique_column("date_col")

    exception_table_database_table.set_data_types(
        {
            "date_col": DatatypeEnum.DATE
        }
    )

    return exception_table_database_table