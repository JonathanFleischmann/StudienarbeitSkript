import sys
from data_storage import DataTable
from ExecuteInserts.datatype_enum import DatatypeEnum


def generate_weekdays_database_table_from_gtfs_table(calendar_gtfs_table):
    """
    Diese Funktion extrahiert die Wochentag-Kombinationen aus der GTFS-Tabelle calendar 
    und bildet sie auf die Datenbank-Tabelle weekdays ab.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = calendar_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    necessary_values_found = {
        "monday": False,
        "tuesday": False,
        "wednesday": False,
        "thursday": False,
        "friday": False,
        "saturday": False,
        "sunday": False
    }
    used_columns = []
    for column in gtfs_table_columns:
        if column in necessary_values_found:
            database_table_columns.append(column)
            necessary_values_found[column] = True
            used_columns.append(column)
    
    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle routes gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    weekdays_database_table = DataTable("weekdays", database_table_columns)

    weekdays_database_table.add_unique_column("monday")
    weekdays_database_table.add_unique_column("tuesday")
    weekdays_database_table.add_unique_column("wednesday")
    weekdays_database_table.add_unique_column("thursday")
    weekdays_database_table.add_unique_column("friday")
    weekdays_database_table.add_unique_column("saturday")
    weekdays_database_table.add_unique_column("sunday")

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    weekdays_database_table.set_all_values(
        calendar_gtfs_table.get_distinct_values_of_all_records(used_columns)
    )

    weekdays_database_table.set_data_types(
        {
            "monday": DatatypeEnum.INTEGER,
            "tuesday": DatatypeEnum.INTEGER,
            "wednesday": DatatypeEnum.INTEGER,
            "thursday": DatatypeEnum.INTEGER,
            "friday": DatatypeEnum.INTEGER,
            "saturday": DatatypeEnum.INTEGER,
            "sunday": DatatypeEnum.INTEGER
        }
    )

    return weekdays_database_table