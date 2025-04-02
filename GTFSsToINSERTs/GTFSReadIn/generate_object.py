import sys
import os
from GTFSReadIn.core import clean_and_split_line, get_value_position_map_from_array, shorten_values_to_relevant, get_ids_from_filename

def add_gtfs_table_to_db_from_filepath(filepath, filename, database_conn, stop_thread_var):
    try:
        total_size = os.path.getsize(filepath)
        calculate_progress = total_size >= 1000000

        if calculate_progress:
            with open(filepath, "r", encoding="utf-8-sig") as file:
                sample_lines = [file.readline() for _ in range(300)]
                avg_line_length_utf8 = sum(len(line.encode('utf-8')) for line in sample_lines) / len(sample_lines)

        with open(filepath, "r", encoding="utf-8-sig") as file:
            columns = clean_and_split_line(file.readline())
            column_position = shorten_values_to_relevant(
                get_value_position_map_from_array(columns), filename
            )

            id_positions = [column_position[column] for column in get_ids_from_filename(filename) if column in column_position]

            create_table_sql = f"CREATE TABLE IF NOT EXISTS {filename} (record_id TEXT PRIMARY KEY, "
            create_table_sql += ", ".join(f"{column} TEXT" for column in column_position.keys()) + ");"
            database_conn.execute(create_table_sql)

            database_conn.execute("PRAGMA synchronous = OFF")
            database_conn.execute("PRAGMA journal_mode = MEMORY")
            database_conn.execute("BEGIN TRANSACTION")

            batch = []
            linecount = 0
            processed_size = 0

            for line in file:
                if linecount % 1000 == 0 and stop_thread_var.get():
                    return

                if calculate_progress:
                    processed_size += avg_line_length_utf8
                    if linecount % 100000 == 0:
                        progress = (processed_size / total_size) * 100
                        print(f"\rDatei **{filename}**: geschätzter Fortschritt: {progress:.2f}%")

                values = clean_and_split_line(line)
                id = "".join(str(values[id_position]) for id_position in id_positions)
                relevant_values = [values[position] for position in column_position.values()]
                batch.append((id, *relevant_values))

                linecount += 1

                if len(batch) >= 100:
                    insert_sql = f"INSERT OR REPLACE INTO {filename} (record_id, {', '.join(column_position.keys())}) VALUES (?, {', '.join(['?' for _ in column_position])})"
                    database_conn.executemany(insert_sql, batch)
                    batch = []

            if batch:
                insert_sql = f"INSERT OR REPLACE INTO {filename} (record_id, {', '.join(column_position.keys())}) VALUES (?, {', '.join(['?' for _ in column_position])})"
                database_conn.executemany(insert_sql, batch)

            database_conn.commit()

    except Exception as e:
        print(f"Fehler beim Verarbeiten der Datei **{filepath}**: {e}", file=sys.stderr)