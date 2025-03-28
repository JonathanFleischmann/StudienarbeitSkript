import os
import copy
import sys
import sqlite3
import queue
import threading
from datatype_enum import DatatypeEnum
from ExecuteInserts.core import get_str_array, map_to_date, map_to_datetime, get_datatypes_for_table, get_unique_columns_for_table

class DataTable:
    MAX_RECORDS_IN_MEMORY = 1_000_000  # Grenze für Datensätze im RAM
    BATCH_SIZE = 200_000  # Größe der Batches für die Datenbankabfragen

    # table_name speichert den Namen der Tabelle
    # columns speichert die Spaltennamen
    # column_values speichert die tatsächlichen Werte mit dem Index der Zeile als Key
    # data_types speichert allen möglichen Datentypen der Spalten
    # unique_columns speichert die Kombinationen von Spalten, die unique sind

    def __init__(self, table_name, columns, is_gtfs_table = False):
        """
        Konstruktor, der die Spaltennamen initialisiert, die Map für die Datensätze erstellt, 
        die Spalten, die zusammen unique sind und die Datentypen der möglichen Spalten speichert. 
        :param table_name: Der Name der Tabelle
        :param columns: Liste der Spaltennamen (Array)
        """
        self.table_name = table_name
        self.columns = columns  # Array mit den Spaltennamen
        self.values = {}  # Map (Dictionary) mit ID als Key und Array der Werte als Value
        self.data_types: dict[str,DatatypeEnum] = get_datatypes_for_table(table_name) if not is_gtfs_table else {} # Map mit den Datentypen der möglichen Spalten
        self.unique_columns: list = get_unique_columns_for_table(table_name) if not is_gtfs_table else [] # Array mit den Spalten, die zusammen unique sind
        self.db_file = f".temp/{self.table_name}.db"  # Datei für ausgelagerte Datensätze
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialisiert die SQLite-Datenbank."""
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()
        columns_definition = ", ".join([f"{col} TEXT" for col in self.columns])
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} (record_id TEXT PRIMARY KEY, {columns_definition})")
        self.conn.commit()

    def get_table_name(self):
        return self.table_name
    
    def get_columns(self):
        return self.columns
    
    def get_unique_colums_sorted(self):
        """
        Gibt die Spalten, die zusammen unique sind, in der Reihenfolge der eigentlichen Spaltennamen zurück.
        :return: Die Spalten, die zusammen unique sind
        """
        sorted_unique_columns = []
        for column in self.columns:
            if column in self.unique_columns:
                sorted_unique_columns.append(column)
        unique_columns_without_value = [column for column in self.unique_columns if column not in self.columns]
        for column in unique_columns_without_value:
            sorted_unique_columns.append(column)
        return sorted_unique_columns


    def __iter__(self):
        """Initialisiert den Batch-Iterator."""
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        self.offset = 0
        return self

    def __next__(self):
        """Lädt den nächsten Batch von Datensätzen."""
        self.cursor.execute(
            f"SELECT * FROM {self.table_name} LIMIT {self.BATCH_SIZE} OFFSET {self.offset}"
        )
        rows = self.cursor.fetchall()
        if not rows:
            raise StopIteration  # Ende der Iteration
        self.offset += self.BATCH_SIZE
        return rows

    def get_all_records(self):
        """
        Gibt alle Datensätze in der Map zurück.
        :return: Die Map mit allen Datensätzen
        """
        return self.values
    
    
    def get_record(self, record_id):
        """
        Gibt die Werte eines Datensatzes zurück.
        :param record_id: Die ID des Datensatzes
        :return: Die Werte des Datensatzes
        :raises KeyError: Wenn die ID nicht existiert
        """
        if record_id not in self.values:
            return None
        return self.values[record_id]
    

    def get_distinct_values_of_all_records(self, columns):
        """
        Gibt die Werte der angegebenen Spalte als Map mit der ID des Datensatzes als Key zurück.
        :param column: Der Name der Spalte
        :return: Die Werte der Spalten als Map mit der ID des Datensatzes als Key
        :raises KeyError: Wenn eine Spalte nicht existiert
        """
        for column in columns:
            if column not in self.columns:
                raise KeyError(f"Die Spalte '{column}' existiert nicht in der Tabelle {self.table_name}.")
        if len(columns) == len(self.columns):
            return copy.deepcopy(self.values)
        column_values = {}
        for record_id, record in self.values.items():
            column_values[record_id] = [copy.deepcopy(record[self.columns.index(column)]) for column in columns]
        return column_values
    

    def get_value(self, record_id, column):
            """
            Gibt den Wert einer bestimmten Spalte eines Datensatzes zurück.
            :param record_id: Die ID des Datensatzes
            :param column: Der Name der Spalte (muss in den Spaltennamen enthalten sein)
            :return: Der Wert der Spalte
            :raises KeyError: Wenn die ID oder die Spalte nicht existiert oder der Wert nicht gefunden wurde
            """
            if record_id not in self.values:
                raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
            if column not in self.columns:
                raise KeyError(f"Die Spalte '{column}' existiert nicht.")
            
            try:
                # Index der Spalte in den Spaltennamen finden
                index = self.columns.index(column)
                return self.values[record_id][index]
            except KeyError: 
                print(f"Der Wert für die Spalte {column} des Datensatzes {record_id} wurde nicht gefunden.", file=sys.stderr)
                return None
            
    def get_record_ids_start_like(self, string_value):
        """
        Gibt die IDs zurück, die mit einem bestimmten String beginnen.
        :param string_value: Der String, mit dem die IDs beginnen sollen
        :return: Die IDs, die mit dem String beginnen
        """
        record_ids = []
        for record_id in self.values:
            if record_id.startswith(string_value):
                record_ids.append(record_id)
        return record_ids

    
    def get_map_with_column_as_key_and_id_as_value(self, column):
        """
        Gibt eine Map zurück, die die Werte einer Spalte als Key hat.
        :param column: Der Name der Spalte
        :return: Die Map mit den Werten der Spalte als Key und den IDs als Value
        :raises KeyError: Wenn die Spalte nicht existiert
        """
        if column not in self.columns:
            raise KeyError(f"Die Spalte '{column}' existiert nicht.")
        column_index = self.columns.index(column)
        column_values = {}
        for record_id, record in self.values.items():
            # Wenn noch kein Wert für den Key existiert, lege ein Array an, ansonsten fügen wir den Wert hinzu
            if record[column_index] == "":
                continue
            if record[column_index] not in column_values:
                column_values[record[column_index]] = [record_id]
            else:
                column_values[record[column_index]].append(record_id)
        return column_values

    def get_record_number(self):
        """
        Gibt die Anzahl der Datensätze zurück.
        :return: Die Anzahl der Datensätze
        """
        return len(self.values)




    ## TODO: replace
    def set_all_values(self, values):
        """
        Setzt die Werte der Spalten für die gesamte Tabelle.
        :param column_values: Map mit den Werten der Spalten
        """
        self.values = values


    def add_values(self, values_to_add: dict[str, list]):
        """
        Fügt Werte in die Map ein oder in die Datenbank, wenn die Anzahl der Datensätze die Grenze überschreitet.
        :param values: Map mit den Werten der Spalten
        """
        records_in_memory = len(self.values)
        if records_in_memory >= self.MAX_RECORDS_IN_MEMORY:
            # Wenn die Grenze erreicht ist, schreibe die neuen Werte mit einem separaten thread in die Datenbank
            threading.Thread(target=self.write_values_to_db, args=(values_to_add,)).start()
            return
        elif len(self.values) + len(values_to_add) > self.MAX_RECORDS_IN_MEMORY:
            # Wenn die Grenze mit den neuen Werten zusammen überschritten wird, schreibe die Werte, 
            # die zu viel in values_to_add sind mit einem separaten thread in die Datenbank und den Rest in die Map self.values
            values_to_store_in_memory = dict(list(values_to_add.items())[:self.MAX_RECORDS_IN_MEMORY - len(self.values)])
            values_to_store_in_db = dict(list(values_to_add.items())[self.MAX_RECORDS_IN_MEMORY - len(self.values):])
            threading.Thread(target=self.write_values_to_db, args=(values_to_store_in_db,)).start()
            for record_id, record in values_to_store_in_memory.items():
                self.values[record_id] = record
        # Wenn die Grenze nicht überschritten wird, füge die Werte in die Map im memory ein
        for record_id, record in values_to_add.items():
            if record_id in self.values:
                self.values[record_id] = record

    def write_values_to_db(self, values_to_write: dict[str, list]):
        """
        Schreibt die Werte in die Datenbank in batches von maximal 1000 Datensätzen unterteilt.
        :param values: Map mit den Werten der Spalten
        """
        insert_sql = f"INSERT INTO {self.table_name} (record_id, {', '.join(self.columns)}) VALUES (?, {', '.join(['?' for _ in range(len(self.columns))])})"
        batch: list[list[str]] = []
        for record_id, record in values_to_write.items():
            batch.append([record_id] + record)
        # Wenn die Batchgröße größer als 1000 ist, teile sie in kleinere Batches auf
        for i in range(0, len(batch), 1000):
            self.cursor.executemany(insert_sql, batch[i:i + 1000])
        self.conn.commit()


    def add_column(self, column_name):
        """
        Fügt eine Spalte in die Map und die Datenbank ein, wenn sie noch nicht existiert.
        :param column_name: Der Name der Spalte, die hinzugefügt werden soll
        :raises KeyError: Wenn die Spalte bereits existiert
        """
        if column_name in self.columns:
            raise KeyError(f"Die Spalte '{column_name}' existiert bereits.")
        self.columns.append(column_name)
        # Füge die Spalte in der Datenbank hinzu
        self.cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} TEXT")
    
    def add_record(self, record_id, values):
        """
        Fügt einen Datensatz in die Map oder die Datenbank ein, wenn die Länge der Werte mit der Anzahl der Spalten übereinstimmt.
        :param record_id: Die ID des Datensatzes (Key in der Map)
        :param values: Array mit den Werten des Datensatzes
        :raises ValueError: Wenn die Länge der Werte nicht mit der Anzahl der Spalten übereinstimmt
        """
        if len(values) != len(self.columns):
            raise ValueError(f"Der Datensatz benötigt {len(self.columns)} Werte ({self.columns}), aber {len(values)} wurden angegeben ({values}).")
        if len(self.values) >= self.MAX_RECORDS_IN_MEMORY:
            # Wenn die Grenze erreicht ist, schreibe die neuen Werte mit einem separaten thread in die Datenbank
            threading.Thread(target=self.write_values_to_db, args={record_id: values}).start()
            return
        # Wenn die Grenze nicht überschritten wird, füge die Werte in die Map im memory ein
        self.values[record_id] = values

    # TODO: Check Latenz
    def set_value(self, record_id, column, value):
        """
        Setzt den Wert einer Spalte eines Datensatzes.
        :param record_id: Die ID des Datensatzes
        :param column: Der Name der Spalte (muss in den Spaltennamen enthalten sein)
        :param value: Der Wert, der gesetzt werden soll
        :raises KeyError: Wenn die ID oder die Spalte nicht existieren oder Werte für manche Spalten fehlen
        """
        if column not in self.columns:
            raise KeyError(f"Es wurde versucht in die Tabelle {self.table_name} für den Datensatz {record_id} den Wert der Spalte {column} zu setzen, aber die Spalte existiert nicht.")
        
        # Wenn der Datensatz in der Map ist, setze den Wert in der Map
        if record_id in self.values:
            # Index der Spalte in den Spaltennamen finden
            index = self.columns.index(column)

            if index > len(self.values[record_id]):
                raise KeyError(f"Es wurde versucht in die Tabelle {self.table_name} für den Datensatz {record_id} den Wert der Spalte {column} zu setzen, aber es fehlen Werte für den Datensatz für manche Spalten.")
            elif index == len(self.values[record_id]):
                self.values[record_id].append(value)
            else:
                self.values[record_id][index] = value

        # Wenn der Datensatz nicht in der Map ist, versuche ihn in der Datenbank zu ändern
        else:
            self.cursor.execute(f"UPDATE {self.table_name} SET {column} = ? WHERE record_id = ?", (value, record_id))
            # schaue, ob ein Datensatz geändert wurde
            if self.cursor.rowcount == 0:
                raise KeyError(f"Es wurde versucht in die Tabelle {self.table_name} für den Datensatz {record_id} den Wert der Spalte {column} zu setzen, aber der Datensatz existiert nicht.")
            self.conn.commit()
    
    def delete_record(self, record_id):
        """
        Löscht einen Datensatz aus der Map oder der Datenbank.
        :param record_id: Die ID des Datensatzes, der gelöscht werden soll
        :raises KeyError: Wenn die ID nicht existiert
        """
        if record_id in self.values:
            del self.values[record_id]
        else:
            # Lösche den Datensatz aus der Datenbank
            self.cursor.execute(f"DELETE FROM {self.table_name} WHERE record_id = ?", (record_id,))
            # schaue, ob ein Datensatz gelöscht wurde
            if self.cursor.rowcount != 1:
                # Wenn kein Datensatz gelöscht wurde, existiert er nicht
                raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
            self.conn.commit()


    def generate_inserts_array(self):
        """
        Diese Funktion gibt die Tupel für die INSERT-Statements zurück.
        Beispiel: [(1, 'Test', 13), (2, 'Test2', 14)]
        :return: Die Tupel für die INSERT-Statements
        :raises KeyError: Wenn der Datentyp für eine Spalte nicht gefunden wurde
        """
        for column in self.columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")

        insert_tuples = []
        # generiere die INSERT-Statements für jeden Datensatz, füge bei TEXT Werten Anführungszeichen hinzu
        for record_id, record in self.values.items():
            new_tuple = tuple()
            for index, value in enumerate(record):
                if value in ('', None):
                    new_tuple += (None,)
                elif self.data_types[self.columns[index]] == DatatypeEnum.TEXT:
                    new_tuple += (value.replace("'", "").replace('"', ''),)
                elif self.data_types[self.columns[index]] == DatatypeEnum.INTEGER:
                    new_tuple += (int(value),)
                elif self.data_types[self.columns[index]] == DatatypeEnum.FLOAT:
                    new_tuple += (float(value),)
                elif self.data_types[self.columns[index]] == DatatypeEnum.DATE:
                    new_tuple += (map_to_date(value),)
                elif self.data_types[self.columns[index]] == DatatypeEnum.TIME:
                    new_tuple += (map_to_datetime(value),)
                else:
                    new_tuple += (value,)
            insert_tuples.append(new_tuple)
        return insert_tuples
    
    def inserts_array_iterator(self):
        """
        Dieser Iterator gibt die Tupel für die INSERT-Statements in batches zurück.
        Beispiel: [(1, 'Test', 13), (2, 'Test2', 14)]
        :return: Die Tupel für die INSERT-Statements
        :raises KeyError: Wenn der Datentyp für eine Spalte nicht gefunden wurde
        """
        for column in self.columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")

        values_array = []
        for record_id, record in self.values.items():
            values_array.append(record)
        return self.generate_tupels_array_from_arrays_array_and_apply_datatypes(values_array)

        
    def inserts_array_iterator(self):
        """
        Iterator, der die Tupel für die INSERT-Statements in Batches von 200.000 Elementen zurückgibt.
        Lädt die nächsten Batches vorab in einem separaten Thread.
        """

        for column in self.columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")

        def prefetch_batches():
            """
            Lädt die Batches vorab und legt sie in die Queue.
            """
            # Zuerst aus `self.values` laden (nur die Values, ohne Keys)
            values_list = list(self.values.values())
            for i in range(0, len(values_list), self.BATCH_SIZE):
                batch = values_list[i:i + self.BATCH_SIZE]
                # Wende die Methode an, um die Datentypen zu konvertieren
                processed_batch = self.generate_tupels_array_from_arrays_array_and_apply_datatypes(batch, self.columns)
                batch_queue.put(processed_batch)

            # Danach aus der Datenbank laden (nur die Spalten in `self.columns`, ohne `record_id`)
            offset = 0
            while True:
                self.cursor.execute(
                    f"SELECT {', '.join(self.columns)} FROM {self.table_name} LIMIT {self.BATCH_SIZE} OFFSET {offset}"
                )
                rows = self.cursor.fetchall()
                if not rows:
                    break
                # Wende die Methode an, um die Datentypen zu konvertieren
                processed_batch = self.generate_tupels_array_from_arrays_array_and_apply_datatypes(rows, self.columns)
                batch_queue.put(processed_batch)
                offset += self.BATCH_SIZE

            # Markiere das Ende der Daten
            batch_queue.put(None)

        batch_queue = queue.Queue(maxsize=2)  # Queue für Prefetching (max. 2 Batches gleichzeitig)
        prefetch_thread = threading.Thread(target=prefetch_batches, daemon=True)
        prefetch_thread.start()

        while True:
            batch = batch_queue.get()  # Warte auf den nächsten Batch
            if batch is None:  # Ende der Daten
                break
            yield batch
        

    def generate_selects_map(self):
        """
        Diese Funktion gibt eine Map zurück, die die record_id als Key hat und die unique_columns in einem tuple als Value.
        Beispiel: {"23112432": (1, 'Test', 13), "31242341": (2, 'Test2', 14)}
        :return: Die Map mit den unique_columns als Value und der record_id als Key
        :raises KeyError: Wenn der Datentyp für eine Spalte nicht gefunden wurde oder die unique_columns nicht in den Spalten existieren
        """

        for column in self.columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")
            
        # generiere eine map, die jedem Index zuordnet, ob der Wert in der Spalte unique ist
        unique_columns_map = {}
        for index, column in enumerate(self.columns):
            if column in self.unique_columns:
                unique_columns_map[index] = True
            else:
                unique_columns_map[index] = False

        unique_columns_without_value = [column for column in self.unique_columns if column not in self.columns]

        select_map = {}
        # generiere die INSERT-Statements für jeden Datensatz, füge bei TEXT Werten Anführungszeichen hinzu
        for record_id, record in self.values.items():
            new_tuple = tuple()
            for index, value in enumerate(record):
                if unique_columns_map[index]:
                    if value in ('', None):
                        new_tuple += (None,)
                    elif self.data_types[self.columns[index]] == DatatypeEnum.TEXT:
                        new_tuple += (value.replace("'", "").replace('"', ''),)
                    elif self.data_types[self.columns[index]] == DatatypeEnum.INTEGER:
                        new_tuple += (int(value),)
                    elif self.data_types[self.columns[index]] == DatatypeEnum.FLOAT:
                        new_tuple += (float(value),)
                    elif self.data_types[self.columns[index]] == DatatypeEnum.DATE:
                        new_tuple += (map_to_date(value),)
                    elif self.data_types[self.columns[index]] == DatatypeEnum.TIME:
                        new_tuple += (map_to_datetime(value),)
                    else:
                        new_tuple += (value,)
            for column in unique_columns_without_value:
                new_tuple += (None,)
            select_map[record_id] = new_tuple
        return select_map
    

    def selects_map_iterator(self):
        """
        Iterator, der die Tupel für die INSERT-Statements in Batches von 200.000 Elementen zurückgibt.
        Lädt die nächsten Batches vorab in einem separaten Thread.
        """

        for column in self.columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")
        
        def prefetch_batches():
            """
            Lädt die Batches vorab und legt sie in die Queue.
            """
            # generiere eine map, die jedem Index zuordnet, ob der Wert in der Spalte unique ist
            unique_columns_map = {}
            for index, column in enumerate(self.columns):
                if column in self.unique_columns:
                    unique_columns_map[index] = True
                else:
                    unique_columns_map[index] = False

            unique_values_map = {}
            # nur die unique_columns in der Map speichern
            for record_id, record in self.values.items():
                new_tuple = tuple()
                for index, value in enumerate(record):
                    if unique_columns_map[index]:
                        if value in ('', None):
                            new_tuple += (None,)
                        else:
                            new_tuple += (value,)
                unique_values_map[record_id] = new_tuple
                

            for i in range(0, len(values_list), self.BATCH_SIZE):
                batch = values_list[i:i + self.BATCH_SIZE]
                for record_id, record in batch:
                    for index, value in enumerate(record):
                        if unique_columns_map[index]:
                            if value in ('', None):
                                record[index] = None
                # Wende die Methode an, um die Datentypen zu konvertieren
                processed_batch = self.generate_tupels_array_from_arrays_array_and_apply_datatypes(batch)
                batch_queue.put(processed_batch)

            # Danach aus der Datenbank laden (nur die Spalten in `self.columns`, ohne `record_id`)
            offset = 0
            while True:
                self.cursor.execute(
                    f"SELECT {', '.join(self.columns)} FROM {self.table_name} LIMIT {self.BATCH_SIZE} OFFSET {offset}"
                )
                rows = self.cursor.fetchall()
                if not rows:
                    break
                # Wende die Methode an, um die Datentypen zu konvertieren
                processed_batch = self.generate_tupels_array_from_arrays_array_and_apply_datatypes(rows)
                batch_queue.put(processed_batch)
                offset += self.BATCH_SIZE

            # Markiere das Ende der Daten
            batch_queue.put(None)

        batch_queue = queue.Queue(maxsize=2)  # Queue für Prefetching (max. 2 Batches gleichzeitig)
        prefetch_thread = threading.Thread(target=prefetch_batches, daemon=True)
        prefetch_thread.start()

        while True:
            batch = batch_queue.get()  # Warte auf den nächsten Batch
            if batch is None:  # Ende der Daten
                break
            yield batch


    def generate_tupels_array_from_arrays_array_and_apply_datatypes(self, arrays_array, columns):
        """
        Diese Funktion gibt das Arrays-Array als Tupel-Array zurück und wendet die Datentypen an.
        Beispiel: [(1, 'Test', 13), (2, 'Test2', 14)]
        :param arrays_array: Das Arrays-Array
        :return: Das Tupel-Array
        """

        data_type_values = []
        for column in columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")
            data_type_values.append(self.data_types[column])
            
            

        insert_tuples = []
        for array in arrays_array:
            new_tuple = tuple()
            for index, value in enumerate(array):
                if value in ('', None):
                    new_tuple += (None,)
                elif data_type_values[index] == DatatypeEnum.TEXT:
                    new_tuple += (value.replace("'", "").replace('"', ''),)
                elif data_type_values[index] == DatatypeEnum.INTEGER:
                    new_tuple += (int(value),)
                elif data_type_values[index] == DatatypeEnum.FLOAT:
                    new_tuple += (float(value),)
                elif data_type_values[index] == DatatypeEnum.DATE:
                    new_tuple += (map_to_date(value),)
                elif data_type_values[index] == DatatypeEnum.TIME:
                    new_tuple += (map_to_datetime(value),)
                else:
                    new_tuple += (value,)
            insert_tuples.append(new_tuple)
        return insert_tuples
    

    def generate_tupels_map_from_arrays_map_and_apply_datatypes(self, arrays_array, columns):
        """
        Diese Funktion gibt die Arrays-Map als Tupel-Map zurück und wendet die Datentypen an.
        Beispiel: {"23112432": (1, 'Test', 13), "31242341": (2, 'Test2', 14)}
        :param arrays_array: Die Arrays-Map
        :param columns: Die Spaltennamen
        :return: Die Tupel-Map
        """
        data_type_values = []
        for column in columns:
            if column not in self.data_types:
                raise KeyError(f"Der Datentyp für die Spalte {column} wurde nicht gefunden.")
            data_type_values.append(self.data_types[column])

        insert_tuples = {}
        for record_id, array in arrays_array.items():
            new_tuple = tuple()
            for index, value in enumerate(array):
                if value in ('', None):
                    new_tuple += (None,)
                elif data_type_values[index] == DatatypeEnum.TEXT:
                    new_tuple += (value.replace("'", "").replace('"', ''),)
                elif data_type_values[index] == DatatypeEnum.INTEGER:
                    new_tuple += (int(value),)
                elif data_type_values[index] == DatatypeEnum.FLOAT:
                    new_tuple += (float(value),)
                elif data_type_values[index] == DatatypeEnum.DATE:
                    new_tuple += (map_to_date(value),)
                elif data_type_values[index] == DatatypeEnum.TIME:
                    new_tuple += (map_to_datetime(value),)
                else:
                    new_tuple += (value,)
            insert_tuples[record_id] = new_tuple
        return insert_tuples