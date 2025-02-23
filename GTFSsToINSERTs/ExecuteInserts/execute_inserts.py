from ExecuteInserts.agency import generate_agency_database_table_from_gtfs_table
from ExecuteInserts.route import generate_route_database_table_from_gtfs_table
from ExecuteInserts.weekdays import generate_weekdays_database_table_from_gtfs_table
from ExecuteInserts.period import generate_period_database_table_from_gtfs_table
from ExecuteInserts.ride import generate_ride_database_table_from_gtfs_tables
from ExecuteInserts.height import generate_height_database_table_from_gtfs_table
from ExecuteInserts.location_type import generate_location_type_database_table
from ExecuteInserts.traffic_centre import generate_traffic_centre_database_table_from_gtfs_tables_and_remove_centres_from_stops
from ExecuteInserts.traffic_point import generate_traffic_point_database_table_from_stops_gtfs_table
from ExecuteInserts.exception_table import generate_exception_table_database_table_from_gtfs_table
from ExecuteInserts.ride_exception import generate_ride_exception_database_table_from_gtfs_tables
from ExecuteInserts.walk_type import generate_walk_type_database_table
from ExecuteInserts.stop_type import generate_stop_type_database_table

from ExecuteInserts.db_executions import do_inserts, select_generated_ids




def execute_inserts(gtfs_table_map, conn):
    """
    Diese Funktion bildet die eingelesenen GTFS-Tabellen auf die Datenbank-Tabellen ab.
    Dabei werden die Fremdschlüssel aufgelöst auf Secundary-Keys der Fremdschlüsseltabelle abgebildet

    :param gtfs_table_map: Eine Map mit den GTFS-Tabellen
    """


    agency_database_table = generate_agency_database_table_from_gtfs_table(gtfs_table_map["agency"])

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



    height_database_table = None

    if "levels" in gtfs_table_map:

        height_database_table = generate_height_database_table_from_gtfs_table(gtfs_table_map["levels"])

        do_inserts(height_database_table, conn)

        select_generated_ids(height_database_table, conn)

    else:

        print("Keine Höheninformationen vorhanden")



    location_type_database_table = generate_location_type_database_table()

    do_inserts(location_type_database_table, conn)

    select_generated_ids(location_type_database_table, conn)
        


    traffic_centre_database_table = generate_traffic_centre_database_table_from_gtfs_tables_and_remove_centres_from_stops(gtfs_table_map["stops"], gtfs_table_map["stop_times"], location_type_database_table)

    do_inserts(traffic_centre_database_table, conn)

    select_generated_ids(traffic_centre_database_table, conn)



    traffic_point_database_table = generate_traffic_point_database_table_from_stops_gtfs_table(gtfs_table_map["stops"], traffic_centre_database_table, location_type_database_table, height_database_table)

    do_inserts(traffic_point_database_table, conn)

    select_generated_ids(traffic_point_database_table, conn)


    
    ride_database_table = generate_ride_database_table_from_gtfs_tables(gtfs_table_map["trips"], period_database_table, route_database_table, gtfs_table_map["stop_times"])

    do_inserts(ride_database_table, conn)

    select_generated_ids(ride_database_table, conn)



    exception_table_database_table = generate_exception_table_database_table_from_gtfs_table(gtfs_table_map["calendar_dates"])
    
    do_inserts(exception_table_database_table, conn)

    select_generated_ids(exception_table_database_table, conn)



    ride_exception_database_table = generate_ride_exception_database_table_from_gtfs_tables(exception_table_database_table, ride_database_table, gtfs_table_map["trips"], gtfs_table_map["calendar_dates"])

    do_inserts(ride_exception_database_table, conn)



    walk_type_database_table = generate_walk_type_database_table()

    do_inserts(walk_type_database_table, conn)

    select_generated_ids(walk_type_database_table, conn)



    stop_type_database_table = generate_stop_type_database_table()

    do_inserts(stop_type_database_table, conn)

    select_generated_ids(stop_type_database_table, conn)