from datatype_enum import DatatypeEnum

relevant_gtfs_files_and_attributes = {
    "agency": ["agency_id", "agency_name", "agency_url"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date", "exception_type"],
    "routes": ["route_id", "route_long_name", "route_short_name", "route_type", "route_desc", "agency_id"],
    "trips": ["trip_id", "route_id", "service_id", "trip_headsign"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id", "pickup_type", "drop_off_type", "stop_headsign", "stop_sequence"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
    "pathways": ["pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "traversal_time"]
}

necessary_attributes_in_relevant_gtfs_files = {
    "pathways": ["pathway_id", "from_stop_id", "to_stop_id", "traversal_time"],
}

necessary_gtfs_files_and_attributes = {
    "agency": ["agency_id", "agency_name"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    "calendar_dates": ["service_id", "date", "exception_type"],
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
    "pathways": ["pathway_id"]
}

column_names_map = {
    "agency": {
        "record_id": ["record_id"],
        "agency_name": ["name"],
        "agency_url": ["url"]
    },
    "deviation": {
        "record_id": ["record_id"],
        "date": ["deviation_date"]
    },
    "segment": {
        "record_id": ["record_id"],
        "trip_id": ["trip_id"],
        "stop_sequence": ["sequence"],
        "stop_id": ["start_point", "end_point"],
        "arrival_time": ["arrival_time"],
        "departure_time": ["departure_time"],
        "pickup_type": ["enter_type"],
        "drop_off_type": ["descend_type"],
        "stop_headsign": ["destination"]
    },
    "walk": {
        "record_id": ["record_id"],
        "from_stop_id": ["start_point"],
        "to_stop_id": ["end_point"],
        "pathway_mode": ["walk_type_id"],
        "is_bidirectional": [],
        "traversal_time": ["min_travel_time"]
    },
    "period": {
        "record_id": ["record_id"],
        "start_date": ["start_date"],
        "end_date": ["end_date"]
    },
    "trip": {
        "record_id": ["record_id"],
        "route_id": ["route_id"],
        "service_id": ["period_id"],
        "trip_headsign": ["trip_headsign"]
    },
    "route": {
        "record_id": ["record_id"],
        "route_long_name": ["name"],
        "route_short_name": ["short_name"],
        "route_type": ["type"],
        "route_desc": ["description"],
        "agency_id": ["agency_id"]
    },
    "traffic_centre": {
        "record_id": ["record_id"],
        "stop_name": ["name"],
        "stop_lat": ["latitude"],
        "stop_lon": ["longitude"],
        "location_type": ["location_type_id"]
    },
    "traffic_point": {
        "record_id": ["record_id"],
        "stop_name": ["name"],
        "stop_lat": ["latitude"],
        "stop_lon": ["longitude"],
        "location_type": ["location_type_id"],
        "parent_station": ["traffic_centre_id"]
    },
    "weekdays": {
        "record_id": ["record_id"],
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
        "url": DatatypeEnum.TEXT
    },
    "deviation": {
        "deviation_date": DatatypeEnum.DATE
    },
    "location_type": {
        "type": DatatypeEnum.TEXT
    },
    "segment": {
        "destination": DatatypeEnum.TEXT,
        "trip_id": DatatypeEnum.INTEGER,
        "start_point": DatatypeEnum.INTEGER,
        "departure_time": DatatypeEnum.TIME,
        "enter_type": DatatypeEnum.INTEGER,
        "end_point": DatatypeEnum.INTEGER,
        "arrival_time": DatatypeEnum.TIME,
        "descend_type": DatatypeEnum.INTEGER,
        "sequence": DatatypeEnum.INTEGER
    },
    "walk": {
        "min_travel_time": DatatypeEnum.INTEGER,
        "walk_type_id": DatatypeEnum.INTEGER,
        "start_point": DatatypeEnum.INTEGER,
        "end_point": DatatypeEnum.INTEGER
    },
    "period": {
        "start_date": DatatypeEnum.DATE,
        "end_date": DatatypeEnum.DATE,
        "weekdays_id": DatatypeEnum.INTEGER
    },
    "trip_deviation": {
        "trip_id": DatatypeEnum.INTEGER,
        "deviation_id": DatatypeEnum.INTEGER
    },
    "trip": {
        "route_id": DatatypeEnum.INTEGER,
        "period_id": DatatypeEnum.INTEGER,
        "start_time": DatatypeEnum.TIME
    },
    "route": {
        "name": DatatypeEnum.TEXT,
        "short_name": DatatypeEnum.TEXT,
        "type": DatatypeEnum.TEXT,
        "description": DatatypeEnum.TEXT,
        "agency_id": DatatypeEnum.INTEGER
    },
    "stop_type": {
        "type": DatatypeEnum.TEXT
    },
    "traffic_centre": {
        "name": DatatypeEnum.TEXT,
        "latitude": DatatypeEnum.FLOAT,
        "longitude": DatatypeEnum.FLOAT,
        "location_type_id": DatatypeEnum.INTEGER
    },
    "traffic_point": {
        "name": DatatypeEnum.TEXT,
        "latitude": DatatypeEnum.FLOAT,
        "longitude": DatatypeEnum.FLOAT,
        "location_type_id": DatatypeEnum.INTEGER,
        "traffic_centre_id": DatatypeEnum.INTEGER
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
    "deviation": ["deviation_date"],
    "location_type": ["type"],
    "segment": ["start_point", "end_point", "ride_id"],
    "walk": ["start_point", "end_point", "walk_type_id"],
    "period": ["start_date", "end_date", "weekdays_id"],
    "trip_deviation": ["trip_id", "deviation_id"],
    "trip": ["route_id", "period_id", "start_time"],
    "route": ["name", "short_name", "agency_id"],
    "stop_type": ["type"],
    "traffic_centre": ["latitude", "longitude"],
    "traffic_point": ["latitude", "longitude"],
    "walk_type": ["type"],
    "weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
}