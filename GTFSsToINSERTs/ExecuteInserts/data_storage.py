import os
import sys

from ExecuteInserts.core import get_str_array, get_str


class DatabaseTable:
    # table_name speichert den Namen der Tabelle
    # columns speichert die Spaltennamen
    # values speichert die tatächlichen Werte mit dem Index der Zeile als Key


    def __init__(self, table_name, columns, foreign_key_name=None):
        """
        Konstruktor, der die Spaltennamen initialisiert und die Map für die Datensätze erstellt.
        :param columns: Liste der Spaltennamen (Array)
        """
        self.foreign_key_name = foreign_key_name # Name des Fremdschlüssels der durch diese Tabelle selektiert werden kann -> nur für Foreign-References
        self.table_name = table_name
        self.columns = columns  # Array mit den Spaltennamen
        self.values = {}  # Map (Dictionary) mit ID als Key und Array der Werte als Value
        self.foreign_references = {} # Map mit Foreign_Reference-Objekten, key ist der Name des Fremdschlüssels        
        self.data_types = {} # Map mit den Datentypen der Spalten
        self.unique_columns_list = [] # Array mit Kombinationen von Spalten, die unique zusammen unique sind

    def get_foreign_key_name(self):
        return self.foreign_key_name

    def get_table_name(self):
        return self.table_name
    
    def get_columns(self):
        return self.columns
    
    def get_all_records(self):
        return self.values
    
    def get_record(self, record_id):
        """
        Gibt die Werte eines Datensatzes zurück.
        :param record_id: Die ID des Datensatzes
        :return: Die Werte des Datensatzes
        """
        if record_id not in self.values:
            raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
        return self.values[record_id]
    
    def get_distinct_attributes_of_all_records(self, columns):
        """
        Gibt die Werte der angegebenen Spalte als Map mit der ID des Datensatzes als Key zurück.
        :param column: Der Name der Spalte
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

    def add_unique_columns(self, unique_columns):
        """
        fügt eine neue Kombination von Spalten hinzu, die unique sind
        :param unique_columns: Array mit den Spaltennamen, die gemeinsam unique sind
        """
        for unique_columns_element in self.unique_columns_list:
            if set(unique_columns_element) == set(unique_columns):
                raise KeyError(f"Die Kombination der Spalten {unique_columns} ist bereits als unique definiert.")
        self.unique_columns_list.append(unique_columns)

    def set_foreign_key_name(self, foreign_key_name):
        self.foreign_key_name = foreign_key_name

    def set_all_values(self, values):
        """
        Setzt die Werte der Spalten für die gesamte Tabelle.
        :param column_values: Map mit den Werten der Spalten
        """
        self.values = values

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

        if index == len(self.values[record_id]):
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






    def add_foreign_reference(self, table_name, foreign_key_name, columns):
        if foreign_key_name in self.foreign_references:
            print(f"Der Fremdschlüssel {foreign_key_name} wurde bereits hinzugefügt.", file=sys.stderr)
        else:
            self.foreign_references[foreign_key_name] = DatabaseTable(table_name, columns, foreign_key_name)


    def add_complete_foreign_reference(self, foreign_reference):
        if foreign_reference.get_foreign_key_name() in self.foreign_references:
            print(f"Der Fremdschlüssel {foreign_reference.get_foreign_key_name()} wurde bereits hinzugefügt.", file=sys.stderr)
        else:
            self.foreign_references[foreign_reference.get_foreign_key_name()] = foreign_reference


    def add_foreign_reference_record(self, record_id, foreign_key_name, values):
        try:
            if foreign_key_name in self.foreign_references:
                self.foreign_references[foreign_key_name].add_record(record_id, values)
            else:
                print(f"Der Fremdschlüssel-Select für {foreign_key_name} wurde nicht gefunden.", file=sys.stderr)
        except Exception as e:
            print(f"Fehler beim Hinzufügen des Fremdschlüssels {foreign_key_name} für Datensatz {record_id}: {e}", file=sys.stderr)



    
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




    # def get_all_foreign_references_for_one_record(self, record_id):
    #     foreign_references = {}
    #     for foreign_key_name in self.foreign_references:
    #         foreign_references[foreign_key_name] = self.foreign_references[foreign_key_name].get_distinct_values(record_id)
    #     return foreign_references
    
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
            file.write(";" + "; ".join(self.columns))
            # Schreibe die Informationen über die Foreign-References in die gleiche Zeile
            for foreign_key_name in self.foreign_references:
                file.write("; " + foreign_key_name + " :")
                for column in self.foreign_references[foreign_key_name].get_columns():
                    file.write(";" + self.foreign_references[foreign_key_name].get_table_name() + "." + column)
            file.write("\n")
            # Schreibe die Werte der Datensätze in die CSV-Datei
            for record_id, values in self.values.items():
                # schreibe die values getrennt durch ein Kommma in die Datei
                file.write(record_id + ";" + "; ".join(get_str_array(values)))
                # Schreibe die Werte der Foreign-References in die gleiche Zeile
                for foreign_key_name in self.foreign_references:
                    file.write(";")
                    for value in self.foreign_references[foreign_key_name].get_record(record_id):
                        file.write(";" + get_str(value))
                file.write("\n")



    def generate_inserts(self):
        """
        Diese Funktion generiert die INSERT-Statements für diese Datenbanktabelle.
        """
        insert_statements = []
        start_text = f"INSERT INTO {self.table_name} ({', '.join(self.columns)}) VALUES "
        # generiere die INSERT-Statements für jeden Datensatz, füge bei TEXT Werten Anführungszeichen hinzu
        for record_id, record in self.values.items():
            # insert_statement = start_text + "("
            # for index, value in enumerate(record):
            #     if self.data_types[self.columns[index]] == "TEXT":
            #         insert_statement += "'" + value.replace("'", "''") + "'"
            #     else:
            #         insert_statement += str(value)
            #     if index < len(record) - 1:
            #         insert_statement += ", "
            # insert_statement += ")"
            # insert_statements.append(insert_statement)
            insert_statements.append(f"{start_text}({', '.join([f"'{value.replace("'", "''")}'" if self.data_types[self.columns[index]] == "TEXT" else str(value) for index, value in enumerate(record)])})") 
        return insert_statements
    
    def generate_selects(self):
        """
        Generiert die SELECT-Statements für die generierte ID und gibt diese in einer Map mit der record_id als Key zurück.
        """
        select_statements = {}
        start_text = f"SELECT id FROM {self.table_name} WHERE "
        select_statement = ""

        # erstelle eine Map, die die Position der Spalten anhand ihrer Namen speichert
        column_positions = {}
        for index, column in enumerate(self.columns):
            column_positions[column] = index

        # gehe alle Datensätze durch
        for record_id, record in self.values.items():
            select_statement = start_text

            # gehe alle Kombinationen der unique_columns durch
            for unique_columns_index, unique_columns in enumerate(self.unique_columns_list):

                # generiere die WHERE-Bedingung für die unique_columns
                unique_expression = ""
                for index, unique_column in enumerate(unique_columns):
                    unique_expression += " " + unique_column + " = "
                    # print (unique_column + " = " + str(column_positions[unique_column]))
                    if self.data_types[unique_column] == "TEXT":
                        unique_expression += f"'{record[column_positions[unique_column]].replace("'", "''")}'"
                    else:
                        unique_expression += str(record[column_positions[unique_column]])
                    if index < len(unique_columns) - 1:
                        unique_expression += " AND"
                select_statement += unique_expression
                # print(unique_expression)

                if unique_columns_index < len(self.unique_columns_list) - 1:
                    select_statement += " OR"

            select_statements[record_id] = select_statement
            # print(select_statement)


        return select_statements


    def copy(self):
        '''
        gibt eine Kopie des DatabaseTable-Objekts zurück
        '''
        copy = DatabaseTable(self.table_name, self.columns.copy(), self.foreign_key_name)
        copy.set_all_values(self.values.copy())
        for foreign_key_name in self.foreign_references:
            copy.add_complete_foreign_reference(self.foreign_references[foreign_key_name].copy())
        return copy