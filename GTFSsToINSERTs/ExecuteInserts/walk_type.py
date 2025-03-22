from data_storage import DataTable, DatatypeEnum

def generate_walk_type_database_table():

    walk_type_database_table = DataTable("walk_type", ["type"])

    walk_type_database_table.add_unique_column("type")

    walk_type_database_table.set_all_values(
        {
            '1': ["walkway"],
            '2': ["stairs"],
            '3': ["moving sidewalk/travelator"],
            '4': ["escalator"],
            '5': ["elevator"],
            '6': ["fare gate"],
            '7': ["exit gate"]
        }
    )

    return walk_type_database_table