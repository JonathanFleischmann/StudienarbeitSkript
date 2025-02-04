import os

class GTFSTable:
    # table_name speichert den Namen des gtfs-Files
    # columns speichert die Spaltennamen
    # column_values speichert die tatächlichen Werte mit dem Index der Zeile als Key

    def __init__(self, columns, table_name):
        """
        Konstruktor, der die Spaltennamen initialisiert und die Map für die Datensätze erstellt.
        :param columns: Liste der Spaltennamen (Array)
        """
        self.table_name = table_name
        self.columns = columns  # Array mit den Spaltennamen
        self.column_values = {}  # Map (Dictionary) mit ID als Key und Array der Werte als Value
    

    def get_table_name(self):
        return self.table_name
    

    def get_columns(self):
        return self.columns


    def get_all_records(self):
        """
        Gibt alle Datensätze in der Map zurück.
        :return: Die Map mit allen Datensätzen
        """
        return self.column_values
    
    
    def get_record(self, record_id):
        """
        Gibt die Werte eines Datensatzes zurück.
        :param record_id: Die ID des Datensatzes
        :return: Die Werte des Datensatzes
        :raises KeyError: Wenn die ID nicht existiert
        """
        if record_id not in self.column_values:
            raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
        return self.column_values[record_id]
    

    def get_distinct_attributes_of_all_records(self, columns):
        """
        Gibt die Werte der angegebenen Spalte als Map mit der ID des Datensatzes als Key zurück.
        :param column: Der Name der Spalte
        """
        for column in columns:
            if column not in self.columns:
                raise KeyError(f"Die Spalte '{column}' existiert nicht in der Tabelle {self.table_name}.")
        if len(columns) == len(self.columns):
            return self.column_values
        column_values = {}
        for record_id, record in self.column_values.items():
            column_values[record_id] = [record[self.columns.index(column)] for column in columns]
        return column_values
    

    def get_attribute(self, record_id, attribute_name):
            """
            Gibt den Wert eines bestimmten Attributs eines Datensatzes zurück.
            :param record_id: Die ID des Datensatzes
            :param attribute_name: Der Name des Attributs (muss in den Spaltennamen enthalten sein)
            :return: Der Wert des Attributs
            :raises KeyError: Wenn die ID oder der Attributname nicht existiert
            """
            if record_id not in self.column_values:
                raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
            if attribute_name not in self.columns:
                raise KeyError(f"Das Attribut '{attribute_name}' existiert nicht.")
            
            # Index des Attributs in den Spaltennamen finden
            index = self.columns.index(attribute_name)
            return self.column_values[record_id][index]
    





    
    def add_record(self, record_id, values):
        """
        Fügt einen Datensatz in die Map ein, wenn die Länge der Werte mit der Anzahl der Spalten übereinstimmt.
        :param record_id: Die ID des Datensatzes (Key in der Map)
        :param values: Array mit den Werten des Datensatzes
        :raises ValueError: Wenn die Länge der Werte nicht mit der Anzahl der Spalten übereinstimmt
        """
        if len(values) != len(self.columns):
            raise ValueError(f"Der Datensatz benötigt {len(self.columns)} Werte, aber {len(values)} wurden angegeben.")
        self.column_values[record_id] = values

    
    def set_attribute(self, record_id, attribute_name, value):
        """
        Setzt den Wert eines Attributs eines Datensatzes.
        :param record_id: Die ID des Datensatzes
        :param attribute_name: Der Name des Attributs (muss in den Spaltennamen enthalten sein)
        :param value: Der Wert, der gesetzt werden soll
        :raises KeyError: Wenn die ID oder der Attributname nicht existiert
        """
        if record_id not in self.column_values:
            raise KeyError(f"Kein Datensatz mit ID {record_id} gefunden.")
        if attribute_name not in self.columns:
            raise KeyError(f"Das Attribut '{attribute_name}' existiert nicht.")
        
        # Index des Attributs in den Spaltennamen finden
        index = self.columns.index(attribute_name)

        self.column_values[record_id][index] = value

    
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
        for record_id in self.column_values:
            del self.column_values[record_id][index]
    



    def print(self):
        """
        Gibt die Spaltennamen und alle Datensätze in der Map aus.
        """
        print("Spalten:", self.columns)
        for record_id, values in self.column_values.items():
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
            for record_id, values in self.column_values.items():
                # schreibe die values getrennt durch ein Kommma in die Datei
                file.write(record_id + ";" + "; ".join(values) + "\n")