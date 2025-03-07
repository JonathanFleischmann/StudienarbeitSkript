from GTFSReadIn.GTFSReadIn import get_table_map_from_GTFSs
from ExecuteInserts.execute_inserts import execute_inserts
from database_connection import get_db_connection
from UserInput.user_interface import start_user_interface

def gtfs_to_inserts(
    host: str, 
    port: int, 
    service_name: str, 
    username: str, 
    password: str, 
    gtfs_path: str,
    batch_size: int,
    stop_thread_var
):
    if stop_thread_var.get(): return

    conn = get_db_connection(
        host, 
        port, 
        service_name, 
        username, 
        password
    )
    if stop_thread_var.get(): return

    gtfs_tables = get_table_map_from_GTFSs(
        gtfs_path,
        stop_thread_var
    )
    if stop_thread_var.get(): return

    execute_inserts(gtfs_tables, conn, stop_thread_var, batch_size)
    if stop_thread_var.get(): return
        

if __name__ == "__main__":

    start_user_interface(gtfs_to_inserts)