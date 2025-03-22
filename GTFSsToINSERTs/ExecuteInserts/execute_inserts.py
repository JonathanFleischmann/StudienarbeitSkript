from ExecuteInserts.agency import generate_agency_database_table
from ExecuteInserts.route import generate_route_database_table
from ExecuteInserts.weekdays import generate_weekdays_database_table
from ExecuteInserts.period import generate_period_database_table
from ExecuteInserts.ride import generate_ride_database_table
from ExecuteInserts.height import generate_height_database_table
from ExecuteInserts.location_type import generate_location_type_database_table
from ExecuteInserts.traffic_centre import generate_traffic_centre_database_table
from ExecuteInserts.traffic_point import generate_traffic_point_database_table
from ExecuteInserts.exception_table import generate_exception_table_database_table
from ExecuteInserts.ride_exception import generate_ride_exception_database_table
from ExecuteInserts.walk_type import generate_walk_type_database_table
from ExecuteInserts.stop_type import generate_stop_type_database_table
from ExecuteInserts.path import generate_path_database_table

from ExecuteInserts.db_executions import do_inserts, select_generated_ids



def execute_inserts(gtfs_table_map, conn, stop_thread_var, batch_size=100):
    """
    Diese Funktion bildet die eingelesenen GTFS-Tabellen auf die Datenbank-Tabellen ab.
    Dabei werden die Fremdschlüssel aufgelöst auf Secundary-Keys der Fremdschlüsseltabelle abgebildet

    :param gtfs_table_map: Eine Map mit den GTFS-Tabellen
    """

    if stop_thread_var.get(): return
    agency_database_table = generate_agency_database_table(gtfs_table_map["agency"])
    if stop_thread_var.get(): return
    do_inserts(agency_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(agency_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    route_database_table = generate_route_database_table(gtfs_table_map["routes"], agency_database_table)
    if stop_thread_var.get(): return
    do_inserts(route_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(route_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    weekdays_database_table = generate_weekdays_database_table(gtfs_table_map["calendar"])
    if stop_thread_var.get(): return
    do_inserts(weekdays_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(weekdays_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    period_database_table = generate_period_database_table(gtfs_table_map["calendar"], weekdays_database_table)
    if stop_thread_var.get(): return
    do_inserts(period_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(period_database_table, conn, batch_size, stop_thread_var)


    
    height_database_table = None

    if "levels" in gtfs_table_map:
        if stop_thread_var.get(): return
        height_database_table = generate_height_database_table(gtfs_table_map["levels"])
        if stop_thread_var.get(): return
        do_inserts(height_database_table, conn, batch_size, stop_thread_var)
        if stop_thread_var.get(): return
        select_generated_ids(height_database_table, conn, batch_size, stop_thread_var)

    else:

        print("Keine Höheninformationen vorhanden")


    if stop_thread_var.get(): return
    location_type_database_table = generate_location_type_database_table()
    if stop_thread_var.get(): return
    do_inserts(location_type_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(location_type_database_table, conn, batch_size, stop_thread_var)
        

    if stop_thread_var.get(): return
    traffic_centre_database_table = generate_traffic_centre_database_table(gtfs_table_map["stops"], gtfs_table_map["stop_times"], location_type_database_table)
    if stop_thread_var.get(): return
    do_inserts(traffic_centre_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(traffic_centre_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    traffic_point_database_table = generate_traffic_point_database_table(gtfs_table_map["stops"], traffic_centre_database_table, location_type_database_table, height_database_table)
    if stop_thread_var.get(): return
    do_inserts(traffic_point_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(traffic_point_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    ride_database_table = generate_ride_database_table(gtfs_table_map["trips"], period_database_table, route_database_table, gtfs_table_map["stop_times"])
    if stop_thread_var.get(): return
    do_inserts(ride_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(ride_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    exception_table_database_table = generate_exception_table_database_table(gtfs_table_map["calendar_dates"])
    if stop_thread_var.get(): return
    do_inserts(exception_table_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(exception_table_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    ride_exception_database_table = generate_ride_exception_database_table(exception_table_database_table, ride_database_table, gtfs_table_map["trips"], gtfs_table_map["calendar_dates"])
    if stop_thread_var.get(): return
    do_inserts(ride_exception_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    walk_type_database_table = generate_walk_type_database_table()
    if stop_thread_var.get(): return
    do_inserts(walk_type_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(walk_type_database_table, conn, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    stop_type_database_table = generate_stop_type_database_table()
    if stop_thread_var.get(): return
    do_inserts(stop_type_database_table, conn, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids(stop_type_database_table, conn, batch_size, stop_thread_var)

    if stop_thread_var.get(): return
    pathways_gtfs_table = gtfs_table_map["pathways"] if "pathways" in gtfs_table_map else None
    if stop_thread_var.get(): return
    path_database_table = generate_path_database_table(gtfs_table_map["stop_times"], pathways_gtfs_table, ride_database_table, traffic_point_database_table, stop_type_database_table, walk_type_database_table, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts(path_database_table, conn, batch_size, stop_thread_var)