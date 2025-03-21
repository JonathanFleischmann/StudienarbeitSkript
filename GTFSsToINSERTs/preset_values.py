relevant_gtfs_files_and_attributes = {
    "agency": ["agency_id", "agency_name", "agency_url", "agency_lang"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date"],
    "routes": ["route_id", "route_long_name", "route_short_name", "route_type", "route_desc", "agency_id"],
    "trips": ["trip_id", "route_id", "service_id", "trip_headsign"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id", "pickup_type", "drop_off_type", "stop_headsign", "stop_sequence"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "level_id"],
    "pathways": ["pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "traversal_time"],
    "levels": ["level_id", "level_index", "level_name", "elevation"],
}

necessary_attributes_in_relevant_gtfs_files = {
    "levels": ["level_id", "elevation"],
    "pathways": ["pathway_id", "from_stop_id", "to_stop_id", "pathway_mode"],
}

necessary_gtfs_files_and_attributes = {
    "agency": ["agency_id", "agency_name"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date"],
    "routes": ["route_id", "route_long_name", "route_short_name", "agency_id"],
    "trips": ["trip_id", "route_id", "service_id"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
}

id_values = {
    "agency": ["agency_id"],
    "calendar": ["service_id"],
    "calendar_dates": ["service_id", "date"],
    "routes": ["route_id"],
    "trips": ["trip_id"],
    "stop_times": ["trip_id", "stop_sequence"],
    "stops": ["stop_id"],
    "pathways": ["pathway_id"],
    "levels": ["level_id"],
}

column_names_map = {
    "agency": {
        "agency_name": ["name"],
        "agency_url": ["url"],
        "agency_lang": ["language"]
    },
    "exception_table": {
        "date": ["date_col"]
    },
    "height": {
        "level_name": ["name"],
        "elevation": ["above_sea_level"],
        "level_index": ["floor"]
    },
    "path": {
        "trip_id": ["ride"],
        "stop_sequence": ["sequence"],
        "stop_id": ["start_point", "end_point"],
        "arrival_time": ["arrival_time"],
        "departure_time": ["departure_time"],
        "pickup_type": ["enter_type"],
        "drop_off_type": ["descend_type"],
        "stop_headsign": ["destination"],
        "headsign": ["destination"],
        "from_stop_id": [],
        "to_stop_id": [],
        "pathway_mode": ["walk_type"]
    },
    "period": {
        "start_date": ["start_date"],
        "end_date": ["end_date"]
    },
    "ride": {
        "route_id": ["route"],
        "service_id": ["period"],
        "trip_headsign": ["headsign"]
    },
    "route": {
        "route_long_name": ["name"],
        "route_short_name": ["short_name"],
        "route_type": ["type"],
        "route_desc": ["description"],
        "agency_id": ["agency"]
    },
    "traffic_centre": {
        "stop_name": ["name"],
        "stop_lat": ["latitude"],
        "stop_lon": ["longitude"],
        "location_type": ["location_type"],
        "parent_station": []
    },
    "traffic_point": {
        "stop_name": ["name"],
        "stop_lat": ["latitude"],
        "stop_lon": ["longitude"],
        "location_type": ["location_type"],
        "parent_station": ["traffic_centre"],
        "level_id": ["height"]
    },
    "weekdays": {
        "monday": ["monday"],
        "tuesday": ["tuesday"],
        "wednesday": ["wednesday"],
        "thursday": ["thursday"],
        "friday": ["friday"],
        "saturday": ["saturday"],
        "sunday": ["sunday"]
    }
}