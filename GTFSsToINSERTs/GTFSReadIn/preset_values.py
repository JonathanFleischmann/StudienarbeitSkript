relevant_values = {
    "agency": ["agency_id", "agency_name", "agency_url", "agency_lang"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date"],
    "routes": ["route_id", "route_long_name", "route_short_name", "route_type", "route_desc", "agency_id"],
    "trips": ["trip_id", "route_id", "service_id"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id", "pickup_type", "drop_off_type", "stop_headsign"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "level_id"],
    "pathways": ["pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "traversal_time"],
    "levels": ["level_id", "level_index", "level_name", "elevation"],
}

necessary_values = {
    "agency": ["agency_id", "agency_name"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date"],
    "routes": ["route_id", "route_long_name", "route_short_name", "agency_id"],
    "trips": ["trip_id", "route_id", "service_id"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type"],
}

id_values = {
    "agency": "agency_id",
    "calendar": "service_id",
    "calendar_dates": "service_id",
    "routes": "route_id",
    "trips": "trip_id",
    "stop_times": "trip_id",
    "stops": "stop_id",
    "pathways": "pathway_id",
    "levels": "level_id",
}