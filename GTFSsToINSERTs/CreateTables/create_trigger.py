import cx_Oracle

def create_or_replace_trigger(oracle_db_connection: cx_Oracle.Connection, trigger_name: str, create_or_replace_trigger_statement: str):
    '''
    führt das gegebene CREATE OR REPLACE TRIGGER Statement auf die Datenbank aus
    am Ende wird ausgegeben, ob das Statement erfolgreich ausgeführt wurde oder es einen Fehler gab
    :param oracle_db_connection: die Verbindung zur Oracle Datenbank
    :param create_or_replace_trigger_statement: das CREATE OR REPLACE TRIGGER Statement'
    :param trigger_name: der Name des Triggers
    '''
    cursor = oracle_db_connection.cursor()
    try:
        cursor.execute(create_or_replace_trigger_statement)
        print(f"✅ Der Trigger **{trigger_name}** wurde erfolgreich erstellt.")
        oracle_db_connection.commit()
    except cx_Oracle.DatabaseError as e:
        print(f"Es ist ein Fehler beim Erstellen des Triggers **{trigger_name}** aufgetreten: {e}")
        oracle_db_connection.rollback()
    finally:
        cursor.close()

