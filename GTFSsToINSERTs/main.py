from database_connection import get_orcle_db_connection
from CreateTables.create_all_tables import create_all_tables
from GTFSReadIn.gtfs_read_in import get_database_cache_from_GTFSs
from ExecuteInserts.execute_inserts import execute_inserts
from ExecuteStatements.oracle_execute_statement import oracle_execute_statement
from UserInput.user_interface import start_user_interface
import cx_Oracle
import os
import sqlite3


def _initialize_cache_database() -> sqlite3.Connection:
    """Initialisiert die SQLite-Datenbank."""
    db_file = ".temp/gtfs_cache.db"

    # Ordner für die Datenbank löschen, falls er existiert
    if os.path.exists(db_file):
        try:
            os.remove(db_file)
            print(f"✅ Alte SQLite-Datenbank '{db_file}' wurde gelöscht.")
        except Exception as e:
            print(f"❌ Fehler beim Löschen der alten SQLite-Datenbank '{db_file}': {e}")
    
    # Ordner für die Datenbank erstellen
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    
    # Verbindung zur SQLite-Datenbank herstellen
    conn = sqlite3.connect(db_file)
    conn.commit()
    print(f"✅ SQLite-Datenbank '{db_file}' wurde erfolgreich zum Zwischenspeichern initialisiert.")
    return conn

def _delete_cache_database(db_conn: sqlite3.Connection):
    """Löscht die SQLite-Datenbank."""
    db_conn.close()
    db_conn = None
    db_file = ".temp/gtfs_cache.db"
    try:
        os.remove(db_file)
        print(f"✅ SQLite-Datenbank '{db_file}' wurde erfolgreich gelöscht.")
    except FileNotFoundError:
        print(f"❌ SQLite-Datenbank '{db_file}' nicht gefunden.")
    except Exception as e:
        print(f"❌ Fehler beim Löschen der SQLite-Datenbank '{db_file}': {e}")






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
    cache_db = _initialize_cache_database()
    if stop_thread_var.get(): 
        _delete_cache_database()
        return
    
    try:

        get_database_cache_from_GTFSs(
            cache_db,
            gtfs_path,
            stop_thread_var
        )

        if stop_thread_var.get(): 
            _delete_cache_database()
            return

        execute_inserts(cache_db, oracle_db_connection, stop_thread_var, batch_size)
        _delete_cache_database(cache_db)
    
    except sqlite3.Error as e:
        print(f"❌ SQLite-Fehler (Cache-Datenbank): {e}")
        _delete_cache_database(cache_db)
    except cx_Oracle.DatabaseError as e:
        print(f"❌ Oracle-Fehler: {e}")
        _delete_cache_database(cache_db)
    except Exception as e:
        print(f"❌ Unerwarteter Fehler: {e}")
        _delete_cache_database(cache_db)


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