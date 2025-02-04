from ExecuteInserts.data_storage import DatabaseTable

def generate_location_type_database_table():

    location_type_database_table = DatabaseTable("location_type", ["type"])

    location_type_database_table.add_unique_columns(["type"])

    location_type_database_table.set_all_values(
        {
            '0': ["platform"],
            '1': ["station"],
            '2': ["entrance/exit"],
            '3': ["generic node"],
            '4': ["boarding area"],
        }
    )
    
    location_type_database_table.set_data_types(
        {
            "type": "TEXT"
        }
    )

    return location_type_database_table