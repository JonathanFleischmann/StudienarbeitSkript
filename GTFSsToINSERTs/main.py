from GTFSReadIn.gtfs_read_in import get_table_map_from_GTFSs
from ExecuteInserts.execute_inserts import execute_inserts
from database_connection import get_orcle_db_connection
from UserInput.user_interface import start_user_interface
import cx_Oracle

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
        

if __name__ == "__main__":

    start_user_interface(connect_to_oracle_db, gtfs_to_inserts)