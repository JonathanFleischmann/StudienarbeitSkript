from ExecuteInserts.GenerateDatabase.agency import clear_agency_cache_db_table
from ExecuteInserts.GenerateDatabase.route import clear_route_cache_db_table
from ExecuteInserts.GenerateDatabase.weekdays import create_new_weekdays_cache_db_table
from ExecuteInserts.GenerateDatabase.period import clear_period_cache_db_table
from ExecuteInserts.GenerateDatabase.ride import clear_ride_cache_db_table
from ExecuteInserts.GenerateDatabase.height import clear_height_cache_db_table
from ExecuteInserts.GenerateDatabase.location_type import create_new_location_type_cache_db_table
from ExecuteInserts.GenerateDatabase.traffic_centre import create_new_traffic_centre_cache_db_table
from ExecuteInserts.GenerateDatabase.traffic_point import clear_traffic_point_cache_db_table
from ExecuteInserts.GenerateDatabase.exception_table import clear_exception_table_cache_db_table
from ExecuteInserts.GenerateDatabase.ride_exception import create_new_ride_exception_cache_db_table
from ExecuteInserts.GenerateDatabase.walk_type import create_new_walk_type_cache_db_table
from ExecuteInserts.GenerateDatabase.stop_type import create_new_stop_type_cache_db_table
from ExecuteInserts.GenerateDatabase.path import clear_path_cache_db_table

from ExecuteInserts.db_executions import do_inserts, select_generated_ids



def execute_inserts(cache_db, oracle_db_connection, stop_thread_var, batch_size=100):
    """
    Diese Funktion überträgt die in die Cache-Datenbank eingelesenen GTFS-Tabellen in die Oracle-Datenbank.
    :param gtfs_table_map: Eine Map mit den GTFS-Tabellen
    """

    if stop_thread_var.get(): return
    clear_agency_cache_db_table(cache_db)
    if stop_thread_var.get(): return
    do_inserts("agency", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("agency", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    clear_route_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("route", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("route", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    create_new_weekdays_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("weekdays", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("weekdays", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    clear_period_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("period", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("period", cache_db, oracle_db_connection, batch_size, stop_thread_var)



    add_height_database_table: bool = cache_db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='levels'").fetchone()[0] == 1

    if add_height_database_table:
        if stop_thread_var.get(): return
        clear_height_cache_db_table(cache_db)
        if stop_thread_var.get(): return
        do_inserts("levels", cache_db, oracle_db_connection, batch_size, stop_thread_var)
        if stop_thread_var.get(): return
        select_generated_ids("levels", cache_db, oracle_db_connection, batch_size, stop_thread_var)

    else:

        print("Keine Höheninformationen vorhanden")


    if stop_thread_var.get(): return
    create_new_location_type_cache_db_table(cache_db)
    if stop_thread_var.get(): return
    do_inserts("location_type", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("location_type", cache_db, oracle_db_connection, batch_size, stop_thread_var)
        

    if stop_thread_var.get(): return
    create_new_traffic_centre_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("traffic_centre", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("traffic_centre", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    clear_traffic_point_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("traffic_point", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("traffic_point", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    clear_ride_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("ride", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("ride", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    clear_exception_table_cache_db_table(cache_db)
    if stop_thread_var.get(): return
    do_inserts("exception_table", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("exception_table", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    create_new_ride_exception_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("ride_exception", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    create_new_walk_type_cache_db_table()
    if stop_thread_var.get(): return
    do_inserts("walk_type", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("walk_type", cache_db, oracle_db_connection, batch_size, stop_thread_var)


    if stop_thread_var.get(): return
    create_new_stop_type_cache_db_table()
    if stop_thread_var.get(): return
    do_inserts("stop_type", cache_db, oracle_db_connection, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    select_generated_ids("stop_type", cache_db, oracle_db_connection, batch_size, stop_thread_var)

    if stop_thread_var.get(): return
    clear_path_cache_db_table(cache_db, batch_size, stop_thread_var)
    if stop_thread_var.get(): return
    do_inserts("path", cache_db, oracle_db_connection, batch_size, stop_thread_var)

    # Lösche die Cache-Datenbank
    cache_db.close()
    oracle_db_connection.close()

    print("✅ Alle Daten wurden erfolgreich in die Oracle-Datenbank übertragen.")

