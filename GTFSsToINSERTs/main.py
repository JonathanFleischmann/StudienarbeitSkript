from database_connection import get_orcle_db_connection
from CreateTables.create_all_tables import create_all_tables
from GTFSReadIn.gtfs_read_in import get_table_map_from_GTFSs
from ExecuteInserts.execute_inserts import execute_inserts
from ExecuteStatements.oracle_execute_statement import oracle_execute_statement
from UserInput.user_interface import start_user_interface
import cx_Oracle


def connect_to_oracle_db(
    host: str, 
    port: int, 
    service_name: str, 
    username: str, 
    password: str, 
)-> cx_Oracle.Connection:

    return get_orcle_db_connection(
        host, 
        port, 
        service_name, 
        username, 
        password
    )


def create_tables_and_triggers(
        oracle_db_connection: cx_Oracle.Connection,
        delete_existing_tables: bool,
        stop_thread_var
        ):
    
    if stop_thread_var.get(): return

    create_all_tables(oracle_db_connection, delete_existing_tables, stop_thread_var)


def gtfs_to_inserts(
    oracle_db_connection: cx_Oracle.Connection,
    gtfs_path: str,
    batch_size: int,
    stop_thread_var
):
    if stop_thread_var.get(): return

    gtfs_tables = get_table_map_from_GTFSs(
        gtfs_path,
        stop_thread_var
    )
    if stop_thread_var.get(): return

    execute_inserts(gtfs_tables, oracle_db_connection, stop_thread_var, batch_size)
    if stop_thread_var.get(): return


def execute_statement_on_oracle_db(
    oracle_db_connection: cx_Oracle.Connection,
    statement: str
):
    return oracle_execute_statement(oracle_db_connection, statement)




if __name__ == "__main__":

    start_user_interface(
        connect_to_oracle_db, 
        create_tables_and_triggers, 
        gtfs_to_inserts, 
        execute_statement_on_oracle_db
    )