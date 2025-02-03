import cx_Oracle
import sys
import os
from ExecuteInserts.data_storage import DatabaseTable

def do_inserts(db_table, conn):
    """
    Führt die Inserts für eine Tabelle durch und schreibt Fehlermeldungen sowie erfolgreiche Inserts in eine Datei.
    
    :param db_table: Die Tabelle, für die die Inserts durchgeführt werden sollen.
    :param conn: Die Datenbankverbindung.
    :param log_file: Der Pfad zur Log-Datei.
    """
    try:
        os.mkdir("logs")
    except FileExistsError:
        pass
    try:
        os.mkdir("logs/" + db_table.get_table_name())
    except FileExistsError:
        pass
    try:
        cur = conn.cursor()
        success_count = 0
        failure_count = 0
        skip_count = 0

        with open("logs/" + db_table.get_table_name() + "/inserts_log_file.txt", "w", encoding="utf-8") as log:
            with open("logs/" + db_table.get_table_name() + "/inserts_error_file.txt", "w", encoding="utf-8") as error_log:
                log.write(f"\nInserts für Tabelle {db_table.get_table_name()} gestartet.\n\n")
                print(f"Inserts für Tabelle {db_table.get_table_name()} gestartet.")

                # Führe die Inserts durch
                inserts = db_table.generate_inserts()
                print(f"Anzahl Inserts: {len(inserts)}")
                for insert in inserts:
                    try:
                        cur.execute(insert)
                        success_count += 1
                        log.write(f"✅ Erfolgreich: {insert}\n")
                    except cx_Oracle.DatabaseError as e:
                        error_code = e.args[0].code
                        error_message = str(e)
                        if error_code == 1:
                            log.write(f"⚠️ FEHLER: {insert}\n    ➝ Datensatz existiert bereits in der Datenbank.\n")
                            skip_count += 1
                        else:
                            log.write(f"❌ FEHLER: {insert}\n    ➝ {error_message}\n")
                            error_log.write(f"❌ FEHLER: {insert}\n    ➝ {error_message}\n")
                            failure_count += 1

                # Commit nur, wenn mindestens ein Insert erfolgreich war
                if success_count > 0:
                    conn.commit()
                    log.write(f"\n✅ {success_count} Inserts erfolgreich.\n")
                else:
                    conn.rollback()
                    log.write(f"\n❌ Alle Inserts sind fehlgeschlagen. Änderungen wurden zurückgesetzt.\n")
                    error_log.write(f"\n❌ Alle Inserts sind fehlgeschlagen. Änderungen wurden zurückgesetzt.\n")

                log.write(f"\n✅ {success_count} Inserts erfolgreich.\n")
                log.write(f"\n⚠️  {skip_count} Inserts waren bereits vorhanden und wurden übersprungen.\n")
                log.write(f"\n❌ {failure_count} Inserts sind fehlgeschlagen.\n")
                error_log.write(f"\n❌ {failure_count} Inserts sind fehlgeschlagen.\n")
                if success_count > 0:
                    print(f"✅ {success_count} Inserts erfolgreich.")
                if skip_count > 0:
                    print(f"⚠️  {skip_count} Inserts waren bereits vorhanden und wurden übersprungen.")
                if failure_count > 0:
                    print(f"❌ {failure_count} Inserts sind fehlgeschlagen.")

            cur.close()
            print(f"Inserts für Tabelle {db_table.get_table_name()} abgeschlossen. Siehe Log-Dateien: {"logs/" + db_table.get_table_name() + "/inserts_log_file.txt"} und {"logs/" + db_table.get_table_name() + "/inserts_error_file.txt"}\n")
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {str(e)}\n")
        raise e


def select_generated_ids(db_table, conn):
    """
    Führt ein SELECT auf die generierten IDs aus und schreibt diese in das db_table-Objekt.
    
    :param db_table: Die Tabelle, für die die generierten IDs ermittelt werden sollen.
    :param conn: Die Datenbankverbindung.
    """
    try:
        os.mkdir("logs")
    except FileExistsError:
        pass
    try:
        os.mkdir("logs/" + db_table.get_table_name())
    except FileExistsError:
        pass
    try:
        cur = conn.cursor()

        with open("logs/" + db_table.get_table_name() + "/id_selects_log_file.txt", "w", encoding="utf-8") as log:
            log.write(f"\nIDs der Tabelle {db_table.get_table_name()} ermitteln.\n\n")
            print(f"IDs der Tabelle {db_table.get_table_name()} ermitteln.")

            # Führe die Selects durch
            selects = db_table.generate_selects()
            print(f"Anzahl Selects: {len(selects.keys())}")
            id_map = {}
            for record_id, select in selects.items():
                try:
                    cur.execute(select)
                    result = cur.fetchone()
                    if result is not None:
                        log.write(f"✅ Erfolgreich: {select}\n")
                        id_map[record_id] = result[0]
                    else:
                        log.write(f"❌ FEHLER: {select}\n    ➝ Kein Ergebnis gefunden.\n")
                        sys.stderr.write(f"❌ FEHLER: {select}\n    ➝ Kein Ergebnis gefunden.\n")
                        sys.exit(1)
                except cx_Oracle.DatabaseError as e:
                    error_message = str(e)
                    log.write(f"❌ FEHLER: {select}\n    ➝ {error_message}\n")
                    sys.stderr.write(f"❌ FEHLER: {select}\n    ➝ {error_message}\n")
                    raise e
            db_table.add_column("id")
            for record_id, id_value in id_map.items():
                db_table.set_value(record_id, "id", id_value)

        cur.close()
        print(f"✅ IDs für Tabelle {db_table.get_table_name()} erfolgrich ermittelt. Siehe Log-Datei: {"logs/" + db_table.get_table_name() + "/id_selects_log_file.txt"}\n")
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {str(e)}\n")
        raise e