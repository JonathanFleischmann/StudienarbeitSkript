import os
import copy
import sys
from ExecuteInserts.datatype_enum import DatatypeEnum

from ExecuteInserts.core import get_str_array, map_to_date, map_to_datetime

class DataTable:
    # table_name speichert den Namen der Tabelle
    # columns speichert die Spaltennamen
    # column_values speichert die tatsächlichen Werte mit dem Index der Zeile als Key
    # data_types speichert die Datentypen der Spalten
    # unique_columns speichert die Kombinationen von Spalten, die unique sind

    def __init__(self, table_name, columns):
        """
        Konstruktor, der die Spaltennamen initialisiert und die Map für die Datensätze erstellt.
        :param columns: Liste der Spaltennamen (Array)
        """
        self.table_name = table_name
        self.columns = columns  # Array mit den Spaltennamen
        self.values = {}  # Map (Dictionary) mit ID als Key und Array der Werte als Value
        self.data_types = {} # Map mit den Datentypen der Spalten
        self.unique_columns = [] # Array mit den Spalten, die zusammen unique sind
    

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
        return sorted_unique_columns


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





    def set_all_values(self, values):
        """
        Setzt die Werte der Spalten für die gesamte Tabelle.
        :param column_values: Map mit den Werten der Spalten
        """
        self.values = values


    def set_data_types(self, data_types: dict[str,DatatypeEnum]):
        """
        Setzt die Datentypen der Spalten.
        :param data_types: Map mit den Datentypen der Spalten
        """
        self.data_types = data_types


    def add_column(self, column_name):
        """
        Fügt eine Spalte in die Map ein, wenn sie noch nicht existiert.
        :param column_name: Der Name der Spalte, die hinzugefügt werden soll
        :raises KeyError: Wenn die Spalte bereits existiert
        """
        if column_name in self.columns:
            raise KeyError(f"Die Spalte '{column_name}' existiert bereits.")
        self.columns.append(column_name)


    def add_unique_column(self, unique_column):
        """
        fügt eine neue Spalte hinzu, die in Kombination mit den anderen Spalten unique sein muss
        :param unique_column: Spaltenname, der hinzugefügt werden soll
        :raises KeyError: Wenn die Spalte bereits als unique definiert ist
        """
        if unique_column in self.unique_columns:
            raise KeyError(f"Die Spalte {unique_column} ist bereits als unique in der Tabelle {self.table_name} definiert.")
        self.unique_columns.append(unique_column)
    
    
    def add_record(self, record_id, values):
        """
        Fügt einen Datensatz in die Map ein, wenn die Länge der Werte mit der Anzahl der Spalten übereinstimmt.
        :param record_id: Die ID des Datensatzes (Key in der Map)
        :param values: Array mit den Werten des Datensatzes
        :raises ValueError: Wenn die Länge der Werte nicht mit der Anzahl der Spalten übereinstimmt
        """
        if len(values) != len(self.columns):
            raise ValueError(f"Der Datensatz benötigt {len(self.columns)} Werte, aber {len(values)} wurden angegeben.")
        self.values[record_id] = values

    
    def set_value(self, record_id, column, value):
        """
        Setzt den Wert einer Spalte eines Datensatzes.
        :param record_id: Die ID des Datensatzes
        :param column: Der Name der Spalte (muss in den Spaltennamen enthalten sein)
        :param value: Der Wert, der gesetzt werden soll
        :raises KeyError: Wenn die ID oder die Spalte nicht existieren oder Werte für manche Spalten fehlen
        """
        if record_id not in self.values:
            raise KeyError(f"Es wurde versucht in die Tabelle {self.table_name} für den Datensatz {record_id} den Wert der Spalte {column} zu setzen, aber der Datensatz existiert nicht.")
        if column not in self.columns:
            raise KeyError(f"Es wurde versucht in die Tabelle {self.table_name} für den Datensatz {record_id} den Wert der Spalte {column} zu setzen, aber die Spalte existiert nicht.")
        
        # Index der Spalte in den Spaltennamen finden
        index = self.columns.index(column)

        if index > len(self.values[record_id]):
            raise KeyError(f"Es wurde versucht in die Tabelle {self.table_name} für den Datensatz {record_id} den Wert der Spalte {column} zu setzen, aber es fehlen Werte für den Datensatz für manche Spalten.")
        elif index == len(self.values[record_id]):
            self.values[record_id].append(value)
        else:
            self.values[record_id][index] = value

    
    def apply_map_function_to_column(self, column_name, map):
        """
        Wendet eine Map-Funktion auf eine Spalte an.
        :param column_name: Der Name der Spalte, auf die die Map angewendet werden soll
        :param map: Die Map, die angewendet werden soll
        :raises KeyError: Wenn der Spaltenname nicht existiert
        """
        if column_name not in self.columns:
            raise KeyError(f"Die Spalte '{column_name}' existiert nicht.")
        colum_index = self.columns.index(column_name)
        for record_id, record in self.values.items():
            self.values[record_id][colum_index] = map(record[colum_index])

    
    def delete_column(self, column_name):
        """
        Löscht eine Spalte aus der Map und den Spaltennamen.
        :param column_name: Der Name der Spalte, die gelöscht werden soll
        :raises KeyError: Wenn der Spaltenname nicht existiert
        """

        if column_name not in self.columns:
            raise KeyError(f"Die Spalte '{column_name}' existiert nicht.")
        
        # Index der Spalte in den Spaltennamen finden
        index = self.columns.index(column_name)
        del self.columns[index]
        
        # Wert für jede Zeile in der Map löschen
        for record_id in self.values:
            del self.values[record_id][index]
    
    def delete_record(self, record_id):
        """
        Löscht einen Datensatz aus der Map.
        :param record_id: Die ID des Datensatzes, der gelöscht werden soll
        :raises KeyError: Wenn die ID nicht existiert
        """
        if record_id not in self.values:
            raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
        del self.values[record_id]



    def print(self):
        """
        Gibt die Spaltennamen und alle Datensätze in der Map aus.
        """
        print("Spalten:", self.columns)
        for record_id, values in self.values.items():
            print(record_id, values)


    def write_to_csv(self):
        """
        Schreibt die Daten in ein CSV-File.
        """
        # Wenn der Unterordner CSVsFromObject nicht existiert, erstelle ihn
        try:
            os.mkdir("CSVsFromObject")
        except FileExistsError:
            pass
        with open("CSVsFromObject/" + self.table_name + '.csv', "w", encoding="utf-8") as file:
            # Schreibe die Spaltennamen in die erste Zeile der CSV-Datei, davor Platz für die ID
            file.write(";" + "; ".join(self.columns) + "\n")
            # Schreibe die Werte der Datensätze in die CSV-Datei
            for record_id, values in self.values.items():
                # schreibe die values getrennt durch ein Kommma in die Datei
                file.write(record_id + ";" + "; ".join(get_str_array(values)) + "\n")

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
            
        for unique_column in self.unique_columns:
            if unique_column not in self.columns:
                raise KeyError(f"Die Spalte {unique_column} wurde als unique definiert, existiert aber nicht in der Tabelle {self.table_name}.")
            
        # generiere eine map, die jedem Index zuordnet, ob der Wert in der Spalte unique ist
        unique_columns_map = {}
        for index, column in enumerate(self.columns):
            if column in self.unique_columns:
                unique_columns_map[index] = True
            else:
                unique_columns_map[index] = False

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
            select_map[record_id] = new_tuple
        return select_map