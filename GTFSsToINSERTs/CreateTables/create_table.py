import cx_Oracle

def create_table(oracle_db_connection: cx_Oracle.Connection, table_name: str, create_table_statement: str):
    '''
    führt das gegebene CREATE TABLE Statement auf die Datenbank aus, wenn die Tabelle noch nicht existiert
    am Ende wird ausgegeben, ob die Tabelle erfolgreich erstellt wurde oder es einen Fehler gab

    :param oracle_db_connection: die Verbindung zur Oracle Datenbank
    :param create_table_statement: das CREATE TABLE Statement
    :param table_name: der Name der Tabelle
    '''
    cursor = oracle_db_connection.cursor()
    try:
        # Überprüfen, ob die Tabelle existiert
        cursor.execute(f"SELECT table_name FROM user_tables WHERE table_name = '{table_name.upper()}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            # Tabelle erstellen
            cursor.execute(create_table_statement)
            print(f"✅ Die Tabelle **{table_name}** wurde erfolgreich erstellt.")
            
        oracle_db_connection.commit()
    except cx_Oracle.DatabaseError as e:
        print(f"Es ist ein Fehler beim Erstellen der Tabelle **{table_name}** aufgetreten: {e}")
        oracle_db_connection.rollback()
    finally:
        cursor.close()

def delete_table(oracle_db_connection: cx_Oracle.Connection, table_name: str):
    '''
    löscht die Tabelle, falls sie existiert
    am Ende wird ausgegeben, ob die Tabelle erfolgreich gelöscht wurde oder es einen Fehler gab

    :param oracle_db_connection: die Verbindung zur Oracle Datenbank
    :param table_name: der Name der Tabelle
    '''
    cursor = oracle_db_connection.cursor()
    try:
        # Überprüfen, ob die Tabelle existiert
        cursor.execute(f"SELECT table_name FROM user_tables WHERE table_name = '{table_name.upper()}'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            print(f"Die Tabelle **{table_name}** existiert bereits und wird zunächst gelöscht.")
            cursor.execute("DROP TABLE " + table_name.upper())

        oracle_db_connection.commit()
    except cx_Oracle.DatabaseError as e:
        print(f"Es ist ein Fehler beim Erstellen der Tabelle **{table_name}** aufgetreten: {e}")
        oracle_db_connection.rollback()
    finally:
        cursor.close()

