import cx_Oracle

def create_or_replace_table(oracle_db_connection: cx_Oracle.Connection, table_name: str, create_table_statement: str):
    '''
    führt das gegebene CREATE TABLE Statement auf die Datenbank aus
    im Falle, dass die Tabelle bereits existiert, wird sie zuerst gelöscht und dann neu erstellt
    am Ende wird ausgegeben, ob die Tabelle erfolgreich erstellt oder ersetzt wurde oder es einen Fehler gab

    :param oracle_db_connection: die Verbindung zur Oracle Datenbank
    :param create_table_statement: das CREATE TABLE Statement
    :param table_name: der Name der Tabelle
    '''
    cursor = oracle_db_connection.cursor()
    try:
        # Überprüfen, ob die Tabelle existiert
        cursor.execute("SELECT table_name FROM user_tables WHERE table_name = '" + table_name.upper() + "'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            print("Die Tabelle ", table_name, "existiert bereits und wird ersetzt.")
            cursor.execute("DROP TABLE " + table_name.upper())

        # Tabelle erstellen
        cursor.execute(create_table_statement)
        print("✅ Die Tabelle ", table_name, "wurde erfolgreich erstellt.")
        oracle_db_connection.commit()
    except cx_Oracle.DatabaseError as e:
        print(f"Es ist ein Fehler beim Erstellen der Tabelle {table_name} aufgetreten: {e}")
        oracle_db_connection.rollback()
    finally:
        cursor.close()

