from data_storage import DataTable

def generate_location_type_database_table():
    """
    Erstellt die Datenbank-Tabelle 'location_type' mit vordefinierten Werten.
    
    :return: Ein DataTable-Objekt für die Tabelle 'location_type'
    """
    # Erstelle ein DataTable-Objekt für die Tabelle 'location_type'
    location_type_database_table = DataTable("location_type", ["type"])

    # Setze die vordefinierten Werte in die Datenbanktabelle
    location_type_database_table.set_all_values(
        {
            '0': ["platform"],
            '1': ["station"],
            '2': ["entrance/exit"],
            '3': ["generic node"],
            '4': ["boarding area"],
        }
    )

    return location_type_database_table