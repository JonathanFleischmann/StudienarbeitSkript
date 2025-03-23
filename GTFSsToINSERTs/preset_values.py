from datatype_enum import DatatypeEnum

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
    "stops": ["stop_id", "stop_lat", "stop_lon", "location_type"],
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
        "date": ["exception_date"]
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
        "pathway_mode": ["walk_type"],
        "is_bidirectional": [],
        "traversal_time": []
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
        "location_type": ["location_type"]
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

column_datatype_map: dict[str,dict[str,DatatypeEnum]] = {
    "agency": {
        "name": DatatypeEnum.TEXT,
        "url": DatatypeEnum.TEXT,
        "language": DatatypeEnum.TEXT
    },
    "exception_table": {
        "exception_date": DatatypeEnum.DATE
    },
    "height": {
        "name": DatatypeEnum.TEXT,
        "above_sea_level": DatatypeEnum.INTEGER,
        "floor": DatatypeEnum.INTEGER
    },
    "location_type": {
        "type": DatatypeEnum.TEXT
    },
    "path": {
        "min_travel_time": DatatypeEnum.INTEGER,
        "is_ride": DatatypeEnum.INTEGER,
        "destination": DatatypeEnum.TEXT,
        "walk_type": DatatypeEnum.INTEGER,
        "ride": DatatypeEnum.INTEGER,
        "start_point": DatatypeEnum.INTEGER,
        "departure_time": DatatypeEnum.TIME,
        "enter_type": DatatypeEnum.INTEGER,
        "end_point": DatatypeEnum.INTEGER,
        "arrival_time": DatatypeEnum.TIME,
        "descend_type": DatatypeEnum.INTEGER,
        "sequence": DatatypeEnum.INTEGER
    },
    "period": {
        "start_date": DatatypeEnum.DATE,
        "end_date": DatatypeEnum.DATE,
        "weekdays": DatatypeEnum.INTEGER
    },
    "ride_exception": {
        "ride": DatatypeEnum.INTEGER,
        "exception_table": DatatypeEnum.INTEGER
    },
    "ride": {
        "route": DatatypeEnum.INTEGER,
        "period": DatatypeEnum.INTEGER,
        "headsign": DatatypeEnum.TEXT,
        "start_time": DatatypeEnum.TIME
    },
    "route": {
        "name": DatatypeEnum.TEXT,
        "short_name": DatatypeEnum.TEXT,
        "type": DatatypeEnum.TEXT,
        "description": DatatypeEnum.TEXT,
        "agency": DatatypeEnum.INTEGER
    },
    "stop_type": {
        "type": DatatypeEnum.TEXT
    },
    "traffic_centre": {
        "name": DatatypeEnum.TEXT,
        "latitude": DatatypeEnum.FLOAT,
        "longitude": DatatypeEnum.FLOAT,
        "location_type": DatatypeEnum.INTEGER
    },
    "traffic_point": {
        "name": DatatypeEnum.TEXT,
        "latitude": DatatypeEnum.FLOAT,
        "longitude": DatatypeEnum.FLOAT,
        "location_type": DatatypeEnum.INTEGER,
        "traffic_centre": DatatypeEnum.INTEGER,
        "height": DatatypeEnum.INTEGER
    },
    "walk_type": {
        "type": DatatypeEnum.TEXT
    },
    "weekdays": {
        "monday": DatatypeEnum.INTEGER,
        "tuesday": DatatypeEnum.INTEGER,
        "wednesday": DatatypeEnum.INTEGER,
        "thursday": DatatypeEnum.INTEGER,
        "friday": DatatypeEnum.INTEGER,
        "saturday": DatatypeEnum.INTEGER,
        "sunday": DatatypeEnum.INTEGER
    }
}

unique_column_map: dict[list[str]] = {
    "agency": ["name"],
    "exception_table": ["exception_date"],
    "height": ["name", "above_sea_level", "floor"],
    "location_type": ["type"],
    "path": ["start_point", "end_point", "walk_type", "ride"],
    "period": ["start_date", "end_date", "weekdays"],
    "ride_exception": ["ride", "exception_table"],
    "ride": ["route", "period", "start_time"],
    "route": ["name", "agency"],
    "stop_type": ["type"],
    "traffic_centre": ["name", "latitude", "longitude", "location_type"],
    "traffic_point": ["latitude", "longitude"],
    "walk_type": ["type"],
    "weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
}