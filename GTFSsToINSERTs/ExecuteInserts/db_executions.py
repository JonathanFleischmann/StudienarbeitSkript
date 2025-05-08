import cx_Oracle
import os
import time
import sqlite3
from preset_values import unique_column_map
from datatype_enum import DatatypeEnum
from ExecuteInserts.core import map_to_date, map_to_datetime, get_datatypes_for_table

def do_inserts(table_name, cache_db_conn: sqlite3.Connection, oracle_db_conn, batch_size, stop_thread_var):
    """
    Führt die Inserts für eine Tabelle durch und schreibt Fehlermeldungen sowie erfolgreiche Inserts in eine Datei.
    
    :param db_table: Die Tabelle, für die die Inserts durchgeführt werden sollen.
    :param conn: Die Datenbankverbindung.
    """
    try:
        # Ordner für Logs erstellen, falls nicht vorhanden
        os.makedirs(f"logs/{table_name}", exist_ok=True)
        
        # Cursor erstellen
        cur = oracle_db_conn.cursor()

        # hole die Spalten der Tabelle ohne die Spalte 'record_id'
        
        columns_to_insert = cache_db_conn.execute(f"PRAGMA table_info({table_name});").fetchall()
        columns_to_insert = [column[1] for column in columns_to_insert if column[1] != "record_id" and column[1] != "id"
                             and column[1] != "trip_headsign" and column[1] != "service_id"]


        # Zähle die Anzahl der Inserts anhand der Anzahl der Zeilen in der cache-Tabelle
        total_inserts = cache_db_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if total_inserts <= 0:
            print(f"\n\nKeine Daten zum Einfügen in die Datenbank-Tabelle **{table_name}** gefunden.")
            return

        print(f"\n➕ Starte {total_inserts} Inserts für Tabelle **{table_name}**...")

        print(f"  Fortschritt: 0%")

        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns_to_insert)}) VALUES ({', '.join([':' + str(i+1) for i in range(len(columns_to_insert))])})"

        # erstelle das SQL-Statement für das Holen der Daten aus der Cache-Datenbank, selecte nur die Spalten in columns_to_insert
        batches_select_sql = f"SELECT {', '.join(columns_to_insert)} FROM {table_name} LIMIT {batch_size} OFFSET ?"

        batch_errors = []
        unique_skips = []
        unique_constraint_violations = 0
        other_errors = 0
        inserted_count = 0
        start_time = time.time()

        # In Batches verarbeiten
        for i in range(0, total_inserts, batch_size):
            if stop_thread_var.get(): return

            # Hole den aktuellen Batch aus der Cache-Datenbank
            # Ergebnis ist eine Liste von Tupeln der Form [(wert1, wert2, ...), (wert1, wert2, ...), ...]
            if i + batch_size > total_inserts:
                batch_size = total_inserts - i
            select_this_batch_sql = batches_select_sql.replace("?", str(i))
            batch = cache_db_conn.execute(select_this_batch_sql).fetchall()

            # hole die Datentypen für die Tabelle
            column_datatypes = get_datatypes_for_table(table_name)
            # passe die Werte im Batch an die Datentypen an
            tuple_batch_with_correct_datatypes = []
            for row in batch:
                new_row = tuple()
                for k in range(len(row)):
                    if row[k] in ('', None, 'None'):
                        new_row += (None,)
                    elif column_datatypes[columns_to_insert[k]] is DatatypeEnum.TEXT:
                        new_row += (row[k].replace("'", "").replace('"', ''),)
                    elif column_datatypes[columns_to_insert[k]] is DatatypeEnum.INTEGER:
                        new_row += (int(row[k]),)
                    elif column_datatypes[columns_to_insert[k]] is DatatypeEnum.FLOAT:
                        new_row += (float(row[k]),)
                    elif column_datatypes[columns_to_insert[k]] is DatatypeEnum.DATE:
                        new_row += (map_to_date(row[k]),)
                    elif column_datatypes[columns_to_insert[k]] is DatatypeEnum.TIME:
                        new_row += (map_to_datetime(row[k]),)
                    else:
                        raise ValueError(f"Unbekannter Datentyp für Spalte {columns_to_insert[k]}: {column_datatypes[columns_to_insert[k]]}")
                tuple_batch_with_correct_datatypes.append(new_row)

            # führe den Batch-Insert aus
            cur.executemany(insert_sql, tuple_batch_with_correct_datatypes, batcherrors=True)

            # Fehlerbehandlung
            for error in cur.getbatcherrors():
                rownum = error.offset
                if error.code == 1:  # ORA-00001: Unique Constraint Violation
                    unique_constraint_violations += 1
                    unique_skips.append((rownum, error.message, tuple_batch_with_correct_datatypes[rownum]))
                else:
                    other_errors += 1
                    batch_errors.append((rownum, error.message, tuple_batch_with_correct_datatypes[rownum]))
            
            # Erfolgreiche Inserts zählen
            inserted_count += len(tuple_batch_with_correct_datatypes) - len(cur.getbatcherrors())

            # Fortschritt berechnen
            elapsed_time = time.time() - start_time
            progressed_records = i+1
            progress_percent = round((progressed_records / total_inserts) * 100, 1)
            avg_time_per_record = elapsed_time / progressed_records
            estimated_remaining_time = avg_time_per_record * (total_inserts - progressed_records)

            remaining_minutes = int(estimated_remaining_time // 60)
            remaining_seconds = int(estimated_remaining_time % 60)

            print(f"\r  Fortschritt: **{progress_percent}%** | "
                  f"Geschätzte Restzeit: **{remaining_minutes}m {remaining_seconds}s**")
            
            # Nach jedem Batch committen
            oracle_db_conn.commit()

        # Gesamtergebnis ausgeben und protokollieren
        with open(f"logs/" + table_name + "/inserts_log_file.txt", "w", encoding="utf-8") as log:
            print("\r  Ergebnis :                                            ")
            log.write("\n Ergebnis :")

            log.write(f"   ➝ {total_inserts} Inserts insgesamt.\n")

            if inserted_count > 0:
                print(f"   ➝ ✅ Erfolgreiche Inserts: {inserted_count}")
                log.write(f"   ➝ ✅ Erfolgreiche Inserts: {inserted_count}\n")
            if unique_constraint_violations > 0:
                print(f"   ➝ ⚠️ Übersprungene Inserts (Unique Constraint): {unique_constraint_violations}")
                log.write(f"   ➝ ⚠️ Übersprungene Inserts (Unique Constraint): {unique_constraint_violations}\n")
            if other_errors > 0:
                print(f"   ➝ ❌ Fehlgeschlagene Inserts (andere Fehler): {other_errors}")
                log.write(f"   ➝ ❌ Fehlgeschlagene Inserts (andere Fehler): {other_errors}\n")
            
            log.write("Dauer: " + time.strftime("%H:%M:%S", time.gmtime(elapsed_time)) + "\n")

        print(f"✅ Inserts für Tabelle **{table_name}** abgeschlossen.\n")

        # Fehlerprotokoll schreiben
        if batch_errors:
            with open(f"logs/" + table_name + "/inserts_error_file.txt", "w", encoding="utf-8") as error_log:
                for rownum, error_msg, failed_insert in batch_errors:
                    if stop_thread_var.get(): return
                    error_log.write(f"❌ FEHLER in Zeile {rownum}: {error_msg}\n  ➝ SQL: {failed_insert}\n")

        if unique_skips:
            with open(f"logs/" + table_name + "/inserts_unique_skips_file.txt", "w", encoding="utf-8") as unique_log:
                for rownum, error_msg, failed_insert in unique_skips:
                    if stop_thread_var.get(): return
                    unique_log.write(f"⚠️ ÜBERSPRUNGENE Zeile {rownum}: {error_msg}\n  ➝ SQL: {failed_insert}\n")

        cur.close()

    except Exception as e:
        print(f"Ein schwerwiegender Fehler ist aufgetreten: {str(e)}")
        oracle_db_conn.rollback()




def select_generated_ids(table_name, cache_db_conn: sqlite3.Connection, oracle_db_conn, batch_size, stop_thread_var):
    """
    Führt ein SELECT auf die Tabelle des db_table-Objekts aus, bei dem sie alle unique Werte und die dazugehörigen IDs erhält.
    Dann werden für die IDs aus dem Table-Objekt die zugehörigen IDs aus der DB mithilfe der unique Werte gefunden.
    
    :param db_table: Die Tabelle, für die die generierten IDs ermittelt werden sollen.
    :param conn: Die Datenbankverbindung.
    """

    try:
        # Ordner für Logs erstellen, falls nicht vorhanden
        os.makedirs(f"logs/{table_name}", exist_ok=True)
        oracle_db_cur = oracle_db_conn.cursor()

        # Zähle die Anzahl der SELECTS anhand der Anzahl der Zeilen in der cache-Tabelle
        total_selects = cache_db_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        unique_columns = unique_column_map[table_name]
        if not unique_columns:
            raise ValueError(f"Für die Tabelle {table_name} sind keine eindeutigen Spalten definiert.")
        
        # erstelle ein SQL-Statement für das Holen der Daten aus der Cache-Datenbank, 
        # selecte nur die Spalten in unique_columns und die Spalte 'record_id'
        cache_select_sql = f"SELECT {', '.join(unique_columns)}, record_id FROM {table_name} LIMIT {batch_size} OFFSET ?"

        print(f"\t🔍 IDs der Tabelle **{table_name}** ermitteln.")

        print(f"\tFortschritt: 0%")

        # füge die Spalte 'id' zu den Spalten in der cache-Datenbank hinzu
        cache_db_conn.execute(f"ALTER TABLE {table_name} ADD COLUMN id TEXT")
        cache_db_conn.commit()

        update_id_in_cache_db_sql = f"UPDATE {table_name} SET id = :1 WHERE record_id = :2"

        with open("logs/" + table_name + "/id_selects_log_file.txt", "w", encoding="utf-8") as log:

            done_count = 0
            start_time = time.time()

            # hole die Daten aus der Cache-Datenbank in Batches
            for i in range(0, total_selects, batch_size):
                if stop_thread_var.get(): return

                # Hole den aktuellen Batch aus der Cache-Datenbank
                # Ergebnis ist eine Liste von Tupeln der Form [(wert1, wert2, ..., record_id), (wert1, wert2, ..., record_id), ...]
                if i + batch_size > total_selects:
                    batch_size = total_selects - i
                select_this_batch_sql = cache_select_sql.replace("?", str(i))
                batch = cache_db_conn.execute(select_this_batch_sql).fetchall()

                # Generiere das Select-Statement für die notwendigen Werte aus der Oracle-Datenbank
                oracle_select_id_sql = f"SELECT {', '.join(unique_columns)}, id FROM {table_name} WHERE ({', '.join(unique_columns)}) IN ("
                arguments_in_where_statement = 0
                for _ in range(len(batch)):  # Verwende die tatsächliche Batch-Größe
                    oracle_select_id_sql += f"("
                    for _ in range(len(unique_columns)):
                        arguments_in_where_statement += 1
                        oracle_select_id_sql += f":{arguments_in_where_statement},"
                    oracle_select_id_sql = oracle_select_id_sql[:-1] + "),"
                oracle_select_id_sql = oracle_select_id_sql[:-1] + ")"

                # hole die Datentypen für die Tabelle
                column_datatypes = get_datatypes_for_table(table_name)
                # passe die Werte in den Keys der Map an die Datentypen an
                # batch hat die Form [(wert1, wert2, ..., record_id), (wert1, wert2, ..., record_id), ...]
                # unique_columns_with_correct_datatypes_flat_list soll die Form [wert1, wert2, ..., wert1, wert2, ..., ...] bekommen
                # unique_columns_record_id_map soll die Form {(wert1, wert2, ...): [record_id1, record_id2, ...], ...}
                unique_columns_with_correct_datatypes_flat_list = []
                unique_columns_record_id_map = {}
                for j in range(len(batch)):
                    row = list(batch[j])
                    new_row = tuple()
                    for k in range(len(row)-1):  # -1, weil die letzte Spalte die record_id ist
                        if row[k] in ('', None, 'None'):
                            new_row += (None,)
                            unique_columns_with_correct_datatypes_flat_list.append(None)
                        elif column_datatypes[unique_columns[k]] is DatatypeEnum.TEXT:
                            new_row += (row[k].replace("'", "").replace('"', ''),)
                            unique_columns_with_correct_datatypes_flat_list.append(row[k].replace("'", "").replace('"', ''))
                        elif column_datatypes[unique_columns[k]] is DatatypeEnum.INTEGER:
                            new_row += (int(row[k]),)
                            unique_columns_with_correct_datatypes_flat_list.append(int(row[k]))
                        elif column_datatypes[unique_columns[k]] is DatatypeEnum.FLOAT:
                            new_row += (float(row[k]),)
                            unique_columns_with_correct_datatypes_flat_list.append(float(row[k]))
                        elif column_datatypes[unique_columns[k]] is DatatypeEnum.DATE:
                            new_row += (map_to_date(row[k]),)
                            unique_columns_with_correct_datatypes_flat_list.append(map_to_date(row[k]))
                        elif column_datatypes[unique_columns[k]] is DatatypeEnum.TIME:
                            new_row += (map_to_datetime(row[k]),)
                            unique_columns_with_correct_datatypes_flat_list.append(map_to_datetime(row[k]))
                        else:
                            raise ValueError(f"Unbekannter Datentyp für Spalte {unique_columns[k]}: {column_datatypes[unique_columns[k]]}")
                    # wenn noch keine ID mit dieser Kombination an Werten existiert, dann erstelle eine neue Liste
                    if tuple(new_row) not in unique_columns_record_id_map:
                        unique_columns_record_id_map[tuple(new_row)] = []
                    # füge die record_id hinzu
                    unique_columns_record_id_map[tuple(new_row)].append(row[-1])

                # Führe das SELECT auf die Oracle-Datenbank aus
                oracle_db_cur.execute(oracle_select_id_sql, unique_columns_with_correct_datatypes_flat_list)
                result = oracle_db_cur.fetchall()

                # gehe die Ergebnisse durch und hole die record_id zu den jeweiligen 
                # ids aus der Oracle-Datenbank und speichere sie in der id_tuple_list
                id_tuple_list = []
                for row in result:
                    # hole die record_id aus der Map
                    record_ids: list = unique_columns_record_id_map[tuple(row[:-1])]
                    # hole die id aus der Oracle-Datenbank
                    id_value = row[-1]
                    # speichere die record_id und die id in der id_tuple_list als Tupel
                    for record_id in record_ids:
                        id_tuple_list.append((str(id_value), record_id))
                
                cache_db_conn.executemany(update_id_in_cache_db_sql, id_tuple_list)
                cache_db_conn.commit()

                done_count += len(id_tuple_list)

                # Fortschritt berechnen
                elapsed_time = time.time() - start_time
                progressed_records = i+1
                progress_percent = round((progressed_records / total_selects) * 100, 1)
                avg_time_per_record = elapsed_time / progressed_records

                estimated_remaining_time = avg_time_per_record * (total_selects - progressed_records)
                remaining_minutes = int(estimated_remaining_time // 60)
                remaining_seconds = int(estimated_remaining_time % 60)

                print(f"\r\tFortschritt: **{progress_percent}%** | "
                      f"Geschätzte Restzeit: **{remaining_minutes}m {remaining_seconds}s**")

            log.write(f"\n✅ {done_count} IDs erfolgreich ermittelt.\n")
            print(f"\r\t✅ {done_count} IDs für Tabelle **{table_name}** erfolgreich ermittelt.\n")

    except Exception as e:
        print(f"\n\t❌ Fehler bei der Ermittlung der IDs für Tabelle **{table_name}**: {str(e)}\n")
        with open("logs/" + table_name + "/id_selects_log_file.txt", "w", encoding="utf-8") as log:
            log.write(f"\n❌ Fehler bei der Ermittlung der IDs für Tabelle **{table_name}**: {str(e)}\n")
        print(unique_columns_record_id_map)
        raise e