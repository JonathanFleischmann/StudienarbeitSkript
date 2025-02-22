import sys
from ExecuteInserts.data_storage import DatabaseTable


def generate_agency_database_table_from_gtfs_table(agency_gtfs_table):
    """
    Diese Funktion bildet die GTFS-Tabelle agency auf die Datenbank-Tabelle agency ab.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle agency vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = agency_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    necessary_values_found = {
        "agency_name": False
    }
    used_columns = []
    for column in gtfs_table_columns:
        if column == "agency_name":
            database_table_columns.append("name")
            necessary_values_found["agency_name"] = True
            used_columns.append(column)
        elif column == "agency_url":
            database_table_columns.append("url")
            used_columns.append(column)
        elif column == "agency_lang":
            database_table_columns.append("language")
            used_columns.append(column)
        else:
            print(f"Die Spalte {column} wird nicht in der Datenbanktabelle agency abgebildet.", file=sys.stderr)
            sys.exit(1)
    

    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle agency gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle agency
    agency_database_table = DatabaseTable("agency", database_table_columns)

    agency_database_table.add_unique_column("name")

    # Füge die Datensätze der GTFS-Tabelle agency in die Datenbanktabelle ein
    agency_database_table.set_all_values(
        agency_gtfs_table.get_distinct_attributes_of_all_records(used_columns)
    )

    agency_database_table.set_data_types(
        {
            "name": DatatypeEnum.TEXT,
            "url": DatatypeEnum.TEXT,
            "language": DatatypeEnum.TEXT
        }
    )

    return agency_database_table