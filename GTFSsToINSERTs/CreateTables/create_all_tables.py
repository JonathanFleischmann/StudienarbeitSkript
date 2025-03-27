import cx_Oracle

from CreateTables.create_table_and_trigger_statements import create_table_statements, create_or_replace_trigger_statements
from CreateTables.create_table import create_or_replace_table
from CreateTables.create_trigger import create_or_replace_trigger

def create_all_tables(oracle_db_connection: cx_Oracle.Connection, stop_thread_var):
    '''
    Erstellt alle für die Speicherung der GTFS-Daten benötigten Tabellen in der Datenbank.
    :param oracle_db_connection: Die Verbindung zur Oracle-Datenbank.
    '''

    for table_name, create_table_statement in create_table_statements.items():
        if stop_thread_var.get(): return
        create_or_replace_table(oracle_db_connection, table_name, create_table_statement)

    for trigger_name, create_or_replace_trigger_statement in create_or_replace_trigger_statements.items():
        if stop_thread_var.get(): return
        create_or_replace_trigger(oracle_db_connection, trigger_name, create_or_replace_trigger_statement)
