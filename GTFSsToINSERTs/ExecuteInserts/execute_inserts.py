from ExecuteInserts.agency import generate_agency_database_table_from_gtfs_table
from ExecuteInserts.route import generate_route_database_table_from_gtfs_table
from ExecuteInserts.weekdays import generate_weekdays_database_table_from_gtfs_table
from ExecuteInserts.period import generate_period_database_table_from_gtfs_table
from ExecuteInserts.ride import generate_ride_database_table_from_gtfs_tables

from ExecuteInserts.db_executions import do_inserts, select_generated_ids




def execute_inserts(gtfs_table_map, conn):
    """
    Diese Funktion bildet die eingelesenen GTFS-Tabellen auf die Datenbank-Tabellen ab.
    Dabei werden die Fremdschlüssel aufgelöst auf Secundary-Keys der Fremdschlüsseltabelle abgebildet

    :param gtfs_table_map: Eine Map mit den GTFS-Tabellen
    """

    db_tables = {}

    agency_database_table = generate_agency_database_table_from_gtfs_table(gtfs_table_map["agency"])

    # db_tables["agency"] = agency_database_table

    do_inserts(agency_database_table, conn)

    select_generated_ids(agency_database_table, conn)

    route_database_table = generate_route_database_table_from_gtfs_table(gtfs_table_map["routes"], agency_database_table)

    do_inserts(route_database_table, conn)

    select_generated_ids(route_database_table, conn)

    weekdays_database_table = generate_weekdays_database_table_from_gtfs_table(gtfs_table_map["calendar"])

    do_inserts(weekdays_database_table, conn)

    select_generated_ids(weekdays_database_table, conn)

    period_database_table = generate_period_database_table_from_gtfs_table(gtfs_table_map["calendar"], weekdays_database_table)
    
    do_inserts(period_database_table, conn)

    select_generated_ids(period_database_table, conn)

    

    # ride_database_table = generate_ride_database_table_from_gtfs_tables(gtfs_table_map["trips"], period_database_table, route_database_table)

    # do_inserts(ride_database_table, conn)

    # select_generated_ids(ride_database_table, conn)


