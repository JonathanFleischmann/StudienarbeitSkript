import cx_Oracle
import sys
import os
import time
from ExecuteInserts.data_storage import DatabaseTable
    
def do_inserts(db_table, conn, batch_size=30):
    """
    Führt die Inserts für eine Tabelle durch und schreibt Fehlermeldungen sowie erfolgreiche Inserts in eine Datei.
    
    :param db_table: Die Tabelle, für die die Inserts durchgeführt werden sollen.
    :param conn: Die Datenbankverbindung.
    """
    try:
        # Ordner für Logs erstellen, falls nicht vorhanden
        os.makedirs(f"logs/{db_table.get_table_name()}", exist_ok=True)
        
        # Cursor erstellen
        cur = conn.cursor()
        
        inserts = db_table.generate_inserts_array()
        total_inserts = len(inserts)

        if total_inserts <= 0:
            print(f"\n\nKeine Daten zum Einfügen in die Datenbank-Tabelle {db_table.get_table_name()} gefunden.")
            return

        print(f"\n➕ Starte {total_inserts} Inserts für Tabelle {db_table.get_table_name()}...")

        insert_sql = f"INSERT INTO {db_table.get_table_name()} ({', '.join(db_table.get_columns())}) VALUES ({', '.join([':' + str(i+1) for i in range(len(db_table.get_columns()))])})"

        batch_errors = []
        unique_constraint_violations = 0
        other_errors = 0
        inserted_count = 0
        start_time = time.time()

         # In Batches verarbeiten
        for i in range(0, total_inserts, batch_size):
            batch = inserts[i:i + batch_size]

            try:
                cur.executemany(insert_sql, batch, batcherrors=True)
            except cx_Oracle.DatabaseError as e:
                print(f"❌ Schwerwiegender Fehler beim Batch-Insert: {str(e)}\n")

            # Fehlerbehandlung
            for error in cur.getbatcherrors():
                rownum = error.offset
                if error.code == 1:  # ORA-00001: Unique Constraint Violation
                    unique_constraint_violations += 1
                else:
                    other_errors += 1
                    batch_errors.append((rownum, error.message, inserts[rownum]))
            
            # Erfolgreiche Inserts zählen
            inserted_count += len(batch) - len(cur.getbatcherrors())

            # Fortschritt berechnen
            elapsed_time = time.time() - start_time
            progressed_records = i+1
            progress_percent = round((progressed_records / total_inserts) * 100, 1)
            avg_time_per_record = elapsed_time / progressed_records
            estimated_remaining_time = avg_time_per_record * (total_inserts - progressed_records)

            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)

            print(f"\r  Fortschritt: {progress_percent}% | "
                  f"Geschätzte Restzeit: {remaining_minutes}m {remaining_seconds}s        ", end="")
            
            # Nach jedem Batch committen
            conn.commit()

        # Gesamtergebnis ausgeben und protokollieren
        with open(f"logs/" + db_table.get_table_name() + "/inserts_log_file.txt", "w", encoding="utf-8") as log:
            print("\r  Ergebnis :                                            ")
            log.write("\n Ergebnis :")

            log.write(f"   ➝ {total_inserts} Inserts insgesamt.\n")

            if inserted_count > 0:
                print(f"   ➝ ✅ Erfolgreiche Inserts: {inserted_count}")
                log.write(f"   ➝ ✅ Erfolgreiche Inserts: {inserted_count}\n")
            if unique_constraint_violations > 0:
                print(f"   ➝ ⚠️  Übersprungene Inserts (Unique Constraint): {unique_constraint_violations}")
                log.write(f"   ➝ ⚠️  Übersprungene Inserts (Unique Constraint): {unique_constraint_violations}\n")
            if other_errors > 0:
                print(f"   ➝ ❌ Fehlgeschlagene Inserts (andere Fehler): {other_errors}")
                log.write(f"   ➝ ❌ Fehlgeschlagene Inserts (andere Fehler): {other_errors}\n")
            
            log.write("Dauer: " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)) + "\n")

        print(f"✅ Inserts für Tabelle {db_table.get_table_name()} abgeschlossen.\n")

        # Fehlerprotokoll schreiben
        if batch_errors:
            with open(f"logs/" + db_table.get_table_name() + "/inserts_error_file.txt", "w", encoding="utf-8") as error_log:
                for rownum, error_msg, failed_insert in batch_errors:
                    error_log.write(f"❌ FEHLER in Zeile {rownum}: {error_msg}\n  ➝ SQL: {failed_insert}\n")

        cur.close()

    except Exception as e:
        print(f"Ein schwerwiegender Fehler ist aufgetreten: {str(e)}")
        conn.rollback()




def select_generated_ids(db_table, conn):
    """
    Führt ein SELECT auf die Tabelle des db_table-Objekts aus, bei dem sie alle unique Werte und die dazugehörigen IDs erhält.
    Dann werden für die IDs aus dem Table-Objekt die zugehörigen IDs aus der DB mithilfe der unique Werte gefunden.
    
    :param db_table: Die Tabelle, für die die generierten IDs ermittelt werden sollen.
    :param conn: Die Datenbankverbindung.
    """

    try:
        # Ordner für Logs erstellen, falls nicht vorhanden
        os.makedirs(f"logs/{db_table.get_table_name()}", exist_ok=True)
        cur = conn.cursor()

        print(f"\t🔍 IDs der Tabelle {db_table.get_table_name()} ermitteln.")

        # Generiere das Select-Statement für die notwendigen Werte
        select_sql = "SELECT id, " + ", ".join(db_table.get_unique_colums_sorted()) + " FROM " + db_table.get_table_name()

        print(f"\t  SQL: {select_sql}")

        try:
            cur.execute(select_sql)
            result = cur.fetchall()
            cur.close()
        except cx_Oracle.DatabaseError as e:
            error_message = str(e)
            sys.stderr.write(f"❌ FEHLER: {select_sql}\n    ➝ {error_message}\n")
            raise e

        tuple_id_map = {tuple(row[1:]): row[0] for row in result}

        with open("logs/" + db_table.get_table_name() + "/id_selects_log_file.txt", "w", encoding="utf-8") as log:

            success_count = 0
            failure_count = 0

            id_map = {}

            # Führe die Selects durch
            selects_map = db_table.generate_selects_map()

            for record_id, select in selects_map.items():

                generated_id = tuple_id_map.get(select, 'id not found')
                if generated_id != 'id not found':
                    id_map[record_id] = generated_id
                    success_count += 1
                else:
                    log.write(f"❌ FEHLER: {select}\n    ➝ Kein Ergebnis gefunden.\n")
                    failure_count += 1

            if failure_count == 0:
                db_table.add_column("id")
                for record_id, id_value in id_map.items():
                    db_table.set_value(record_id, "id", id_value)
                log.write(f"\n✅ {success_count} IDs erfolgreich ermittelt.\n")
                print(f"\t✅ IDs für Tabelle {db_table.get_table_name()} erfolgreich ermittelt.\n")
            else:
                log.write(f"\nDas ist das Ergebnis aus der Datenbankabfrage: {tuple_id_map}\n")
                log.write(f"\n✅ {success_count} IDs erfolgreich ermittelt.\n")
                log.write(f"\n❌ {failure_count} IDs konnten nicht ermittelt werden.\n")
                print(f"\n\t❌ Fehler bei der Ermittlung der IDs für Tabelle {db_table.get_table_name()}. Siehe Log-Datei: {"logs/" + db_table.get_table_name() + "/id_selects_log_file.txt"}\n")

    except Exception as e:
        print(f"\n\tEin Fehler ist aufgetreten: {str(e)}\n")
        raise e