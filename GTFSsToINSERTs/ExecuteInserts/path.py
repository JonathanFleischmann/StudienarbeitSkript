import copy
import time
from data_storage import DataTable, DatatypeEnum
from ExecuteInserts.core import get_minute_difference, get_time_when_more_than_24_h, append_new_columns_and_get_used


def generate_path_database_table_from_gtfs_tables(stop_times_gtfs_table, pathways_gtfs_table, ride_database_table, traffic_point_database_table, stop_type_database_table, walk_type_database_table, stop_thread_var):
    """
    Diese Funktion 
    """

    stop_times_gtfs_table_columns = stop_times_gtfs_table.get_columns()

    new_and_used_columns_from_stop_times = append_new_columns_and_get_used("path", stop_times_gtfs_table_columns)

    database_table_columns = new_and_used_columns_from_stop_times["new_columns"]
    used_stop_times_columns = new_and_used_columns_from_stop_times["used_columns"]


    database_table_columns.append("is_ride")
    database_table_columns.append("min_travel_time")

    destination_in_stop_times = False
    destination_in_ride = False
    if "stop_headsign" in stop_times_gtfs_table_columns:
        destination_in_stop_times = True
    if "headsign" in ride_database_table.get_columns():
        destination_in_ride = True
        if not destination_in_stop_times:
            database_table_columns.append("destination")

    used_pathways_columns = []
    if pathways_gtfs_table is not None:

        pathways_gtfs_table_columns = pathways_gtfs_table.get_columns()    

        new_and_used_columns_from_pathways = append_new_columns_and_get_used("path", pathways_gtfs_table_columns, already_new_columns = database_table_columns)
        
        database_table_columns = new_and_used_columns_from_pathways["new_columns"]
        used_pathways_columns = new_and_used_columns_from_pathways["used_columns"]

    print("\n_\npath columns: ", database_table_columns, "\n_\n")

    # Erstelle ein DatabaseTable-Objekt für die Tabelle path
    path_database_table = DataTable("path", database_table_columns)
    
    path_database_table.add_unique_column("start_point")
    path_database_table.add_unique_column("end_point")
    path_database_table.add_unique_column("walk_type")
    path_database_table.add_unique_column("ride")

    print("🔨 Generiere Tabelle path...")
    total_datasets = stop_times_gtfs_table.get_record_number()
    if pathways_gtfs_table is not None:
        total_datasets += pathways_gtfs_table.get_record_number()
    progressed_records = 0
    progress_percent = 0
    start_time = time.time()

    path_data = {}

    generated_id = 0

    stop_times_column_positions = {}
    for column in used_stop_times_columns:
        stop_times_column_positions[column] = stop_times_gtfs_table_columns.index(column)

    for record_id, record in stop_times_gtfs_table.get_distinct_values_of_all_records(used_stop_times_columns).items():
        data_map = {}

        ride = record[stop_times_column_positions["trip_id"]]
        data_map["ride"] = ride
        data_map["sequence"] = record[stop_times_column_positions["stop_sequence"]]
        next_path_id = str(ride) + str(int(data_map["sequence"])+1)

        next_path = stop_times_gtfs_table.get_record(next_path_id)

        if next_path is not None:
                
            data_map["is_ride"] = 1
            data_map["walk_type"] = None
            data_map["departure_time"] = get_time_when_more_than_24_h(record[stop_times_column_positions["departure_time"]])
            data_map["arrival_time"] = get_time_when_more_than_24_h(record[stop_times_column_positions["arrival_time"]])
            if destination_in_stop_times:
                data_map["destination"] = record[stop_times_column_positions["stop_headsign"]]
            elif destination_in_ride:
                data_map["destination"] = ride_database_table.get_value(ride, "headsign")
            data_map["min_travel_time"] = get_minute_difference(data_map["arrival_time"], data_map["departure_time"])
            data_map["start_point"] = record[stop_times_column_positions["stop_id"]]
            data_map["end_point"] = next_path[stop_times_column_positions["stop_id"]]
            data_map["enter_type"] = record[stop_times_column_positions["pickup_type"]]
            data_map["descend_type"] = next_path[stop_times_column_positions["drop_off_type"]]

            data = []
            for column in database_table_columns:
                data.append(copy.deepcopy(data_map[column]))

            path_data[generated_id] = data
            generated_id += 1
        
        progressed_records += 1

        if progressed_records % 10000 == 0:
            if stop_thread_var.get(): return
            elapsed_time = time.time() - start_time
            progress_percent = round((progressed_records / (total_datasets + total_datasets * 0.2)) * 100, 1)
            estimated_remaining_time = elapsed_time * 1/(progress_percent/100) - elapsed_time
            
            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)

            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")

    pathways_columns_positions = {}
    for column in used_pathways_columns:
        pathways_columns_positions[column] = pathways_gtfs_table_columns.index(column)

    if pathways_gtfs_table is not None:
        for record_id, record in pathways_gtfs_table.get_distinct_values_of_all_records(used_pathways_columns).items():
            data_map = {}

            data_map["is_ride"] = 0
            data_map["ride"] = None
            data_map["sequence"] = None
            data_map["departure_time"] = None
            data_map["arrival_time"] = None
            data_map["destination"] = None

            data_map["min_travel_time"] = record[pathways_columns_positions["traversal_time"]]
            data_map["walk_type"] = record[pathways_columns_positions["pathway_mode"]]
            data_map["start_point"] = record[pathways_columns_positions["from_stop_id"]]
            data_map["end_point"] = next_path[pathways_columns_positions["to_stop_id"]]

            data = []
            for column in database_table_columns:
                data.append(copy.deepcopy(data_map[column]))

            path_data[generated_id] = data
            generated_id += 1

            if "is_bidirectional" in used_pathways_columns and record[pathways_columns_positions["is_bidirectional"]] == 1:
                data_map["start_point"] = record[pathways_columns_positions["to_stop_id"]]
                data_map["end_point"] = record[pathways_columns_positions["from_stop_id"]]

                data = []
                for column in database_table_columns:
                    data.append(copy.deepcopy(data_map[column]))

                path_data[generated_id] = data
                generated_id += 1

        progressed_records += 1

        if progressed_records % 10000 == 0:
            if stop_thread_var.get(): return
            elapsed_time = time.time() - start_time
            progress_percent = round((progressed_records / (total_datasets + total_datasets * 0.2)) * 100, 1)
            estimated_remaining_time = elapsed_time * 1/(progress_percent/100) - elapsed_time
            
            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)
            
            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")
            
    
    path_database_table.set_all_values(path_data)

    

    progressed_records = 0
    total_datasets = path_database_table.get_record_number() * 3
    if pathways_gtfs_table is not None:
        total_datasets += path_database_table.get_record_number() * 1


    # ersetze die ride-id aus der stop_times-gtfs-file durch die neu generierte id der ride-table
    ride_id_map = ride_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, ride in path_database_table.get_distinct_values_of_all_records(["ride"]).items():
        progressed_records += 1
        if ride[0] is None:
            continue
        ride_new_id = ride_id_map[ride[0]][0]
        path_database_table.set_value(record_id, "ride", ride_new_id)

        if progressed_records % 100000 == 0:
            if stop_thread_var.get(): return
            elapsed_time = time.time() - start_time
            progress_percent = round((0.8 + progressed_records / total_datasets * 0.2) * 100, 1)
            estimated_remaining_time = elapsed_time * 1/(progress_percent/100) - elapsed_time
            
            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)
            
            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")

    
    # ersetze start_point und end_point aus der stop_times-gtfs-file/pathways-gtfs_file durch die neu generierte id der traffic_point-table
    traffic_point_id_map = traffic_point_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, traffic_points in path_database_table.get_distinct_values_of_all_records(["start_point", "end_point"]).items():
        start_point_new_id = traffic_point_id_map[traffic_points[0]][0]
        end_point_new_id = traffic_point_id_map[traffic_points[1]][0]
        path_database_table.set_value(record_id, "start_point", start_point_new_id)
        path_database_table.set_value(record_id, "end_point", end_point_new_id)

        progressed_records += 1

        if progressed_records % 100000 == 0:
            if stop_thread_var.get(): return
            elapsed_time = time.time() - start_time
            progress_percent = round((0.8 + progressed_records / total_datasets * 0.2) * 100, 1)
            estimated_remaining_time = elapsed_time * 1/(progress_percent/100) - elapsed_time
            
            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)
            
            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")

    # ersetze enter_type und descend_type aus der stop_times-gtfs-file durch die neu generierte id der stop_type-table
    stop_type_id_map = stop_type_database_table.get_distinct_values_of_all_records(["id"])
    for record_id, stop_types in path_database_table.get_distinct_values_of_all_records(["enter_type", "descend_type"]).items():
        progressed_records += 1
        if stop_types[0] is None or stop_types[1] is None:
            continue
        enter_type_new_id = stop_type_id_map[stop_types[0]][0]
        descend_type_new_id = stop_type_id_map[stop_types[1]][0]
        path_database_table.set_value(record_id, "enter_type", enter_type_new_id)
        path_database_table.set_value(record_id, "descend_type", descend_type_new_id)

        if progressed_records % 100000 == 0:
            if stop_thread_var.get(): return
            elapsed_time = time.time() - start_time
            progress_percent = round((0.8 + progressed_records / total_datasets * 0.2) * 100, 1)
            estimated_remaining_time = elapsed_time * 1/(progress_percent/100) - elapsed_time
            
            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)
            
            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")

    if pathways_gtfs_table is not None:
        # ersetze walk_type aus der pathways-gtfs-file durch die neu generierte id der walk_type-table
        walk_type_id_map = walk_type_database_table.get_distinct_values_of_all_records(["id"])
        for record_id, walk_type in path_database_table.get_distinct_values_of_all_records(["walk_type"]).items():
            progressed_records += 1
            if walk_type[0] is None:
                continue
            walk_type_new_id = walk_type_id_map[walk_type[0]][0]
            path_database_table.set_value(record_id, "walk_type", walk_type_new_id)

        if progressed_records % 100000 == 0:
            if stop_thread_var.get(): return
            elapsed_time = time.time() - start_time
            progress_percent = round((0.8 + progressed_records / total_datasets * 0.2) * 100, 1)
            estimated_remaining_time = elapsed_time * 1/(progress_percent/100) - elapsed_time
            
            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)
            
            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")

    print("\r✅ Tabelle path generiert.                                                     ")

    return path_database_table