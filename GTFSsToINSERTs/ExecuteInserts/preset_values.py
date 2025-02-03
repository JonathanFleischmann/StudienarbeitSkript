result_values = {
    "agency": ["name", "url", "language"],
    "route": ["name", "short_name", "type", "description", "agency"],
    "ride": ["start_time", "end_time", "repeat_after", "route", "period"],
    "period": ["start_date", "end_date", "weekdays"],
    "weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
    "exception": ["date"],
    "ride_exception": ["ride", "exception"],
    "path": ["min_travel_time", "is_ride", "destination", "walk_type", "ride", "start_point", "departure_time", "enter_type", "end_point", "arrival_time", "descend_type"],
    "stop_type": ["type"],
    "walk_type": ["type"],
    "traffic_point": ["name", "type", "location_type", "latitute", "longitude", "height", "traffic_centre"],
    "location_type": ["type"],
    "height": ["name", "above_sea_level", "floor"],
    "traffic_centre": ["name", "traffic_centre_type", "latitute", "longitude"],
    "traffic_centre_type": ["type"]
}

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

table_names = {
    "agency": "agency",
    "calendar": "service_id",
    "calendar_dates": "service_id",
    "routes": "route_id",
    "trips": "trip_id",
    "stop_times": "trip_id",
    "stops": "stop_id",
    "pathways": "pathway_id",
    "levels": "level_id",
}

no_quotation_marks = {
    "agency_id": True,
    "agency_name": False,
    "agency_url": False,
    "agency_lang": False,
    "service_id": True,
    "monday": True,
    "tuesday": True,
    "wednesday": True,
    "thursday": True,
    "friday": True,
    "saturday": True,
    "sunday": True,
    "start_date": True,
    "end_date": True,
    "route_id": True,
    "route_long_name": False,
    "route_short_name": False,
    "route_type": True,
    "route_desc": False,
    "trip_id": True,
    "service_id": True,
    "arrival_time": True,
    "departure_time": True,
    "stop_id": True,
    "pickup_type": True,
    "drop_off_type": True,
    "stop_headsign": False,
    "stop_name": False,
    "stop_lat": True,
    "stop_lon": True,
    "location_type": True,
    "parent_station": True,
    "level_id": True,
    "pathway_id": True,
    "from_stop_id": True,
    "to_stop_id": True,
    "pathway_mode": True,
    "is_bidirectional": True,
    "traversal_time": True,
    "level_index": True,
    "level_name": False,
    "elevation": True,
}

agency_attribute_names_replace_map = {
    "agency_id": "id",
    "agency_name": "name",
    "agency_url": "url",
    "agency_lang": "language"
}

route_attribute_names_replace_map = {
    "route_id": "id",
    "route_long_name": "name",
    "route_short_name": "short_name",
    "route_type": "type",
    "route_desc": "description",
    "agency_id": "agency"
}

height_attribute_names_replace_map = {
    "level_id": "id",
    "level_index": "floor",
    "elevation": "above_sea_level"
}