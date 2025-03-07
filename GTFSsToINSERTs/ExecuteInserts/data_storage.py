import os
import sys
from ExecuteInserts.datatype_enum import DatatypeEnum

from ExecuteInserts.core import get_str_array, get_str, map_to_date, map_to_datetime


class DatabaseTable:
    # table_name speichert den Namen der Tabelle
    # columns speichert die Spaltennamen
    # values speichert die tatächlichen Werte mit dem Index der Zeile als Key
    # data_types speichert die Datentypen der Spalten
    # unique_columns_list speichert die Kombinationen von Spalten, die unique sind


    def __init__(self, table_name, columns, foreign_key_name=None):
        """
        Konstruktor, der die Spaltennamen initialisiert und die Map für die Datensätze erstellt.
        :param columns: Liste der Spaltennamen (Array)
        """
        self.table_name = table_name
        self.columns = columns  # Array mit den Spaltennamen
        self.values = {}  # Map (Dictionary) mit ID als Key und Array der Werte als Value
        self.data_types = {} # Map mit den Datentypen der Spalten
        self.unique_columns_list = [] # Array mit den Spalten, die zusammen unique sind


    def get_table_name(self):
        return self.table_name
    
    
    def get_columns(self):
        return self.columns
    
    def get_unique_colums_sorted(self):
        sorted_unique_columns = []
        for column in self.columns:
            if column in self.unique_columns_list:
                sorted_unique_columns.append(column)
        return sorted_unique_columns
        

    def get_all_records(self):
        return self.values
    

    def get_record(self, record_id):
        """
        Gibt die Werte eines Datensatzes zurück.
        :param record_id: Die ID des Datensatzes
        :return: Die Werte des Datensatzes
        """
        if record_id not in self.values:
            return None
        return self.values[record_id]
    

    def get_distinct_attributes_of_all_records(self, columns):
        """
        Gibt die Werte der angegebenen Spalte als Map mit der ID des Datensatzes als Key zurück.
        :param columns: Liste der Spaltennamen
        """
        for column in columns:
            if column not in self.columns:
                raise KeyError(f"Die Spalte '{column}' existiert nicht in der Tabelle {self.table_name}.")
        column_values = {}
        for record_id, record in self.values.items():
            column_values[record_id] = [record[self.columns.index(column)] for column in columns]
        return column_values


    def get_value(self, record_id, column):
            """
            Gibt den Wert eines Attributs eines Datensatzes zurück.
            :param record_id: Die ID des Datensatzes
            :param column: Der Name des Attributs (muss in den Spaltennamen enthalten sein)
            """
            try:
                index = self.columns.index(column)
                return self.values[record_id][index]
            except KeyError: 
                print(f"Der Wert für die Spalte {column} des Datensatzes {record_id} wurde nicht gefunden.", file=sys.stderr)
                return None

    def get_map_with_column_as_key_and_id_as_value(self, column):
        """
        Gibt eine Map zurück, die die Werte einer Spalte als Key hat.
        :param column: Der Name der Spalte
        """
        if column not in self.columns:
            raise KeyError(f"Die Spalte '{column}' existiert nicht.")
        column_position = self.columns.index(column)
        column_values = {}
        for record_id, record in self.values.items():
            # Wenn noch kein Wert für den Key existiert, lege ein Array an, ansonsten fügen wir den Wert hinzu
            if record[column_position] not in column_values:
                column_values[record[column_position]] = [record_id]
            else:
                column_values[record[column_position]].append(record_id)
        return column_values
    
    def get_record_number(self):
        """
        Gibt die Anzahl der Datensätze zurück.
        """
        return len(self.values)






    def set_all_values(self, values):
        """
        Setzt die Werte der Spalten für die gesamte Tabelle.
        :param column_values: Map mit den Werten der Spalten
        """
        self.values = values


    def set_data_types(self, data_types):
        """
        Setzt die Datentypen der Spalten.
        :param data_types: Map mit den Datentypen der Spalten
        """
        self.data_types = data_types
    

    def add_column(self, column_name):
        """
        Fügt eine Spalte in die Map ein, wenn sie noch nicht existiert.
        :param column_name: Der Name der Spalte, die hinzugefügt werden soll
        """
        if column_name in self.columns:
            raise KeyError(f"Die Spalte '{column_name}' existiert bereits.")
        self.columns.append(column_name)


    def add_unique_column(self, unique_column):
        """
        fügt eine neue Spalte hinzu, die in Kombination mit den anderen Spalten unique sein muss
        :param unique_column: Spaltenname, der hinzugefügt werden soll
        """
        if unique_column in self.unique_columns_list:
            raise KeyError(f"Die Spalte {unique_column} ist bereits als unique in der Tabelle {self.table_name} definiert.")
        self.unique_columns_list.append(unique_column)


    def add_record(self, record_id, values):
        """
        Fügt einen Datensatz in die Map ein, wenn die Länge der Werte mit der Anzahl der Spalten übereinstimmt.
        :param record_id: Die ID des Datensatzes (Key in der Map)
        :param values: Array mit den Werten des Datensatzes
        """
        if len(values) != len(self.columns):
            raise ValueError(f"Der Datensatz benötigt {len(self.columns)} Werte, aber {len(values)} wurden angegeben.")
        self.values[record_id] = values
    

    def set_value(self, record_id, attribute_name, value):
        """
        Setzt den Wert eines Attributs eines Datensatzes.
        :param record_id: Die ID des Datensatzes
        :param attribute_name: Der Name des Attributs (muss in den Spaltennamen enthalten sein)
        :param value: Der Wert, der gesetzt werden soll
        """
        if record_id not in self.values:
            raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
        if attribute_name not in self.columns:
            raise KeyError(f"Das Attribut '{attribute_name}' existiert nicht.")
        
        # Index des Attributs in den Spaltennamen finden
        index = self.columns.index(attribute_name)

        if index >= len(self.values[record_id]):
            self.values[record_id].append(value)
        else:
            self.values[record_id][index] = value


    def apply_map_function_to_column(self, column_name, map):
        """
        Wendet eine Map-Funktion auf eine Spalte an.
        :param column_name: Der Name der Spalte, auf die die Map angewendet werden soll
        :param map: Die Map, die angewendet werden soll
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
        """

        if column_name not in self.columns:
            raise KeyError(f"Die Spalte '{column_name}' existiert nicht.")
        
        # Index der Spalte in den Spaltennamen finden
        index = self.columns.index(column_name)
        del self.columns[index]
        
        # Wert für jede Zeile in der Map löschen
        for record_id in self.values:
            del self.values[record_id][index]

    




    def write_to_csv(self):
        '''
        Schreibt die Daten in eine CSV-Datei
        '''
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
        """
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
        """
        # generiere eine map, die jedem Index zuordnet, ob der Wert in der Spalte unique ist
        unique_columns_map = {}
        for index, column in enumerate(self.columns):
            if column in self.unique_columns_list:
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