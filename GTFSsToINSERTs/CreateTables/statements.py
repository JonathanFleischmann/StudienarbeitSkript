create_table_statements: dict[str,str] = {
    "agency": 
        '''
        CREATE TABLE agency (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name VARCHAR2(100) UNIQUE NOT NULL,
            url VARCHAR2(100)
        )
        ''',
    "route": 
        '''
        CREATE TABLE route (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name VARCHAR2(200),
            short_name VARCHAR2(10) NOT NULL,
            type VARCHAR2(50),
            description VARCHAR2(400),
            agency_id NUMBER NOT NULL,
            CONSTRAINT unique_route UNIQUE (name, short_name, agency_id),
            CONSTRAINT fk_agency FOREIGN KEY (agency_id) REFERENCES agency(id) ON DELETE CASCADE
        )
        ''',
    "weekdays": 
        '''
        CREATE TABLE weekdays (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            monday NUMBER(1) NOT NULL CHECK (monday IN (0, 1)),
            tuesday NUMBER(1) NOT NULL CHECK (tuesday IN (0, 1)),
            wednesday NUMBER(1) NOT NULL CHECK (wednesday IN (0, 1)),
            thursday NUMBER(1) NOT NULL CHECK (thursday IN (0, 1)),
            friday NUMBER(1) NOT NULL CHECK (friday IN (0, 1)),
            saturday NUMBER(1) NOT NULL CHECK (saturday IN (0, 1)),
            sunday NUMBER(1) NOT NULL CHECK (sunday IN (0, 1)),
            CONSTRAINT unique_weekdays UNIQUE (monday, tuesday, wednesday, thursday, friday, saturday, sunday)
        )
        ''',
    "period":
        '''
        CREATE TABLE period (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            weekdays_id NUMBER NOT NULL,
            CONSTRAINT unique_period UNIQUE (start_date, end_date, weekdays_id),
            CONSTRAINT fk_weekdays FOREIGN KEY (weekdays_id) REFERENCES weekdays(id) ON DELETE CASCADE,
            CONSTRAINT check_dates CHECK (start_date <= end_date)
        )
        ''',
    "trip":
        '''
        CREATE TABLE trip (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            route_id NUMBER NOT NULL,
            period_id NUMBER NOT NULL,
            start_time TIMESTAMP NOT NULL,
            CONSTRAINT unique_trip UNIQUE (route_id, period_id, start_time),
            CONSTRAINT fk_route FOREIGN KEY (route_id) REFERENCES route(id) ON DELETE CASCADE,
            CONSTRAINT fk_period FOREIGN KEY (period_id) REFERENCES period(id) ON DELETE CASCADE
        )
        ''',
    "deviation":
        '''
        CREATE TABLE deviation (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            deviation_date DATE UNIQUE NOT NULL
        )
        ''',
    "trip_deviation":
        '''
        CREATE TABLE trip_deviation (
            trip_id NUMBER NOT NULL,
            deviation_id NUMBER NOT NULL,                                
            CONSTRAINT pk_trip_deviation PRIMARY KEY (trip_id, deviation_id),
            CONSTRAINT fk_trip_in_trip_deviation FOREIGN KEY (trip_id) REFERENCES trip(id) ON DELETE CASCADE,
            CONSTRAINT fk_deviation FOREIGN KEY (deviation_id) REFERENCES deviation(id) ON DELETE CASCADE
        )
        ''',
    "location_type":
        '''
        CREATE TABLE location_type (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            type VARCHAR2(50) UNIQUE NOT NULL
        )
        ''',
    "traffic_centre":
        '''
        CREATE TABLE traffic_centre (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name VARCHAR2(100),
            location_type_id NUMBER NOT NULL,
            latitude NUMBER(20, 15) NOT NULL,
            longitude NUMBER(20, 15) NOT NULL,
            CONSTRAINT unique_traffic_centre UNIQUE (latitude, longitude),
            CONSTRAINT fk_location_type_in_traffic_centre FOREIGN KEY (location_type_id) REFERENCES location_type(id)
        )
        ''',
    "traffic_point":
        '''
        CREATE TABLE traffic_point (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name VARCHAR2(100),
            location_type_id NUMBER,
            latitude NUMBER(20, 15) NOT NULL,
            longitude NUMBER(20, 15) NOT NULL,
            traffic_centre_id NUMBER,
            CONSTRAINT unique_traffic_point UNIQUE (latitude, longitude),
            CONSTRAINT fk_location_type_in_traffic_point FOREIGN KEY (location_type_id) REFERENCES location_type(id),
            CONSTRAINT fk_traffic_centre FOREIGN KEY (traffic_centre_id) REFERENCES traffic_centre(id) ON DELETE CASCADE
        )
        ''',
    "walk_type":
        '''
        CREATE TABLE walk_type (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            type VARCHAR2(50) UNIQUE NOT NULL
        )
        ''',
    "stop_type":
        '''
        CREATE TABLE stop_type (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            type VARCHAR2(50) UNIQUE NOT NULL
        )
        ''',
    "segment":
        '''
        CREATE TABLE segment (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            destination VARCHAR2(200),
            trip_id NUMBER,
            start_point INTEGER NOT NULL,
            departure_time TIMESTAMP NOT NULL,
            enter_type INTEGER,
            end_point INTEGER NOT NULL,
            arrival_time TIMESTAMP NOT NULL,
            descend_type INTEGER,
            sequence INTEGER NOT NULL,
            CONSTRAINT unique_segment UNIQUE (start_point, end_point, trip_id),
            CONSTRAINT fk_trip_in_segment FOREIGN KEY (trip_id) REFERENCES trip(id) ON DELETE CASCADE,
            CONSTRAINT fk_start_point_in_segment FOREIGN KEY (start_point) REFERENCES traffic_point(id) ON DELETE CASCADE,
            CONSTRAINT fk_enter_type FOREIGN KEY (enter_type) REFERENCES stop_type(id) ON DELETE CASCADE,
            CONSTRAINT fk_end_point_in_segment FOREIGN KEY (end_point) REFERENCES traffic_point(id) ON DELETE CASCADE,
            CONSTRAINT fk_descend_type FOREIGN KEY (descend_type) REFERENCES stop_type(id) ON DELETE CASCADE
        )
        ''',
    "walk":
        '''
        CREATE TABLE walk (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            min_travel_time NUMBER NOT NULL,
            walk_type_id NUMBER,
            start_point INTEGER NOT NULL,
            end_point INTEGER NOT NULL,
            CONSTRAINT unique_walk UNIQUE (start_point, end_point, walk_type_id),
            CONSTRAINT fk_walk_type FOREIGN KEY (walk_type_id) REFERENCES walk_type(id),
            CONSTRAINT fk_start_point_in_walk FOREIGN KEY (start_point) REFERENCES traffic_point(id) ON DELETE CASCADE,
            CONSTRAINT fk_end_point_in_walk FOREIGN KEY (end_point) REFERENCES traffic_point(id) ON DELETE CASCADE
        )
        '''
}


delete_table_order: list[str] = [
    "path",
    "walk",
    "segment",
    "stop_type",
    "walk_type",
    "traffic_point",
    "traffic_centre",
    "height",
    "location_type",
    "ride_exception",
    "trip_deviation",
    "exception_table",
    "deviation",
    "ride",
    "trip",
    "period",
    "weekdays",
    "route",
    "agency"
]



create_or_replace_trigger_statements: dict[str,str] = {
    "period_ensure_date_with_default_time":
        '''
        CREATE OR REPLACE TRIGGER period_ensure_date_with_default_time
            BEFORE INSERT OR UPDATE ON period
            FOR EACH ROW
        BEGIN
            :NEW.start_date := TRUNC(:NEW.start_date);
            :NEW.end_date := TRUNC(:NEW.end_date);
        END;
        ''',
    "deviation_ensure_date_with_default_time":
        '''
        CREATE OR REPLACE TRIGGER deviation_ensure_date_with_default_time
            BEFORE INSERT OR UPDATE ON deviation
            FOR EACH ROW
        BEGIN
            :NEW.deviation_date := TRUNC(:NEW.deviation_date);
        END;
        ''',
    "segment_ensure_time_with_default_date":
        '''
        CREATE OR REPLACE TRIGGER segment_ensure_time_with_default_date
            BEFORE INSERT OR UPDATE ON segment
            FOR EACH ROW
        BEGIN
            :NEW.departure_time := TO_TIMESTAMP_TZ('1970-01-01 ' || TO_CHAR(:NEW.departure_time, 'HH24:MI') || ':00 ' || TO_CHAR(:NEW.departure_time, 'TZD'), 'YYYY-MM-DD HH24:MI:SS TZD');
            :NEW.arrival_time := TO_TIMESTAMP_TZ('1970-01-01 ' || TO_CHAR(:NEW.arrival_time, 'HH24:MI') || ':00 ' || TO_CHAR(:NEW.arrival_time, 'TZD'), 'YYYY-MM-DD HH24:MI:SS TZD');
        END;
        ''',
    "drop_old_trigger_1":
        '''
        BEGIN
            EXECUTE IMMEDIATE 'DROP TRIGGER exception_table_ensure_date_with_default_time';
        EXCEPTION
            WHEN OTHERS THEN
                -- Prüfe, ob der Fehler darauf zurückzuführen ist, dass der Trigger nicht existiert
                IF SQLCODE != -4080 THEN -- Fehlercode -4080: Trigger existiert nicht
                    RAISE; -- Anderen Fehler erneut auslösen
                END IF;
        END;
        ''',
        "drop_old_trigger_2":
        '''
        BEGIN
            EXECUTE IMMEDIATE 'DROP TRIGGER path_ensure_time_with_default_date';
        EXCEPTION
            WHEN OTHERS THEN
                -- Prüfe, ob der Fehler darauf zurückzuführen ist, dass der Trigger nicht existiert
                IF SQLCODE != -4080 THEN -- Fehlercode -4080: Trigger existiert nicht
                    RAISE; -- Anderen Fehler erneut auslösen
                END IF;
        END;
        ''',
    "drop_old_triggers_3":
        '''
        BEGIN
            EXECUTE IMMEDIATE 'DROP TRIGGER path_calculate_min_travel_time_if_possible';
        EXCEPTION
            WHEN OTHERS THEN
                -- Prüfe, ob der Fehler darauf zurückzuführen ist, dass der Trigger nicht existiert
                IF SQLCODE != -4080 THEN -- Fehlercode -4080: Trigger existiert nicht
                    RAISE; -- Anderen Fehler erneut auslösen
                END IF;
        END;
        ''',
}