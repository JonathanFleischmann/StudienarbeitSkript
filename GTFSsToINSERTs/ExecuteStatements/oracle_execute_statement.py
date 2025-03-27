import cx_Oracle

def oracle_execute_statement(oracle_db_connection: cx_Oracle.Connection, statement: str):
    '''
    führt das gegebene Statement auf die Datenbank aus
    am Ende wird das Ergebnis zurückgegeben
    :param oracle_db_connection: die Verbindung zur Oracle Datenbank
    :param statement: das Statement
    '''
    cursor = oracle_db_connection.cursor()
    try:
        cursor.execute(statement)

        result = cursor.fetchall()
        return result
    except cx_Oracle.DatabaseError as e:
        oracle_db_connection.rollback()
        return f"❌ Es ist ein Fehler beim Ausführen des Statements aufgetreten: {e}"
    finally:
        cursor.close()