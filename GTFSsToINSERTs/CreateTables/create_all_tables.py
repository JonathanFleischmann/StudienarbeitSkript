import cx_Oracle

from CreateTables.statements import delete_table_order, create_table_statements, create_or_replace_trigger_statements
from CreateTables.maintain_table import create_table, delete_table
from CreateTables.maintain_trigger import create_or_replace_trigger

def create_all_tables(oracle_db_connection: cx_Oracle.Connection, delete_existing_tables: bool, stop_thread_var):
    '''
    Erstellt alle für die Speicherung der GTFS-Daten benötigten Tabellen in der Datenbank.
    :param oracle_db_connection: Die Verbindung zur Oracle-Datenbank.
    '''

    if delete_existing_tables:
        for table_name in delete_table_order:
            if stop_thread_var.get(): return
            delete_table(oracle_db_connection, table_name)

    create_table_count: int = 0
    for table_name, create_table_statement in create_table_statements.items():
        if stop_thread_var.get(): return
        if create_table(oracle_db_connection, table_name, create_table_statement):
            create_table_count += 1

    for trigger_name, create_or_replace_trigger_statement in create_or_replace_trigger_statements.items():
        if stop_thread_var.get(): return
        create_or_replace_trigger(oracle_db_connection, trigger_name, create_or_replace_trigger_statement)

    print(f"✅ Es wurden **{create_table_count}** Tabellen erstellt.")
