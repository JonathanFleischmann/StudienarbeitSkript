import sys
from ExecuteInserts.data_storage import DatabaseTable


def generate_route_database_table_from_gtfs_table(routes_gtfs_table, agency_database_table):
    """
    Diese Funktion bildet die GTFS-Tabelle routes auf die Datenbank-Tabelle route ab mithilfe der Foreign-Reference auf die agency-Tabelle.
    """

    # Finde heraus, welche Spalten in der DatabaseTabelle route vorhanden sein werden anhand der GTFSTabelle
    gtfs_table_columns = routes_gtfs_table.get_columns()
    database_table_columns = []


    # Erstelle eine Liste mit den Spaltennamen der Datenbanktabelle
    necessary_values_found = {
        "route_long_name": False,
        "route_short_name": False,
        "agency_id": False
    }
    used_columns = []
    for column in gtfs_table_columns:
        if column == "route_long_name":
            database_table_columns.append("name")
            necessary_values_found["route_long_name"] = True
            used_columns.append(column)
        elif column == "route_short_name":
            database_table_columns.append("short_name")
            necessary_values_found["route_short_name"] = True
            used_columns.append(column)
        elif column == "route_type":
            database_table_columns.append("type")
            used_columns.append(column)
        elif column == "route_desc":
            database_table_columns.append("description")
            used_columns.append(column)
        elif column == "agency_id":
            database_table_columns.append("agency")
            necessary_values_found["agency_id"] = True
            used_columns.append(column)
        else:
            print(f"Die Spalte {column} wird nicht in der Datenbanktabelle route abgebildet.", file=sys.stderr)
            sys.exit(1)
    
    # Überprüfe 
    for necessary_value, found in necessary_values_found.items():
        if not found:
            print(f"Die Spalte {necessary_value} wurde nicht in der GTFS-Tabelle routes gefunden. Diese Spalte fehlt im GTFS-File", file=sys.stderr)
            sys.exit(1)

    # Erstelle ein DatabaseTable-Objekt für die Tabelle route
    route_database_table = DatabaseTable("route", database_table_columns)

    # Füge die Datensätze der GTFS-Tabelle route in die Datenbanktabelle ein
    route_database_table.set_all_values(
        routes_gtfs_table.get_distinct_attributes_of_all_records(used_columns)
    )

    route_database_table.add_unique_column("name")
    route_database_table.add_unique_column("agency")

    route_database_table.set_data_types(
        {
            "name": "TEXT",
            "short_name": "TEXT",
            "type": "TEXT",
            "description": "TEXT", 
            "agency": "INTEGER"
        }
    )

    # ersetze die agency_id aus dem gtfs-file durch die neu generierte id der agency
    agency_id_map = agency_database_table.get_distinct_attributes_of_all_records(["id"])
    for record_id, agency in route_database_table.get_distinct_attributes_of_all_records(["agency"]).items():
        agency_new_id = agency_id_map[agency[0]][0]
        route_database_table.set_value(record_id, "agency", agency_new_id)

    return route_database_table