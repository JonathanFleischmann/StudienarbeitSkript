from data_storage import DataTable

def generate_walk_type_database_table():
    """
    Erstellt die Datenbank-Tabelle 'walk_type' mit vordefinierten Werten.
    
    :return: Ein DataTable-Objekt für die Tabelle 'walk_type'
    """
    # Erstelle ein DataTable-Objekt für die Tabelle 'walk_type'
    walk_type_database_table = DataTable("walk_type", ["type"])

    # Setze die vordefinierten Werte in die Datenbanktabelle
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