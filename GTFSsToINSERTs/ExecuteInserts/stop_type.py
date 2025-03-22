from data_storage import DataTable

def generate_stop_type_database_table():

    stop_type_database_table = DataTable("stop_type", ["type"])

    stop_type_database_table.set_all_values(
        {
            '0': ["regular stop"],
            '1': ["no service"],
            '2': ["must phone agency to arrange service"],
            '3': ["must coordinate with driver to arrange service"]
        }
    )

    return stop_type_database_table