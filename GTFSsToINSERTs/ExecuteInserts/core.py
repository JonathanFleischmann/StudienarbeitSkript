from datetime import datetime
from preset_values import column_names_map

def get_str_array(value_array):
    """
    Diese Funktion gibt die Werte innerhalb eines arrays als String-array zurück.
    möglich sind numerische, boolsche und String-Werte.
    """
    str_array = []
    for value in value_array:
        if isinstance(value, bool):
            str_array.append(str(value).lower())
        else:
            str_array.append(str(value))
    return str_array

def get_str(value):
    """
    Diese Funktion gibt den Wert als String zurück.
    möglich sind numerische, boolsche und String-Werte.
    """
    if isinstance(value, bool):
        return str(value).lower()
    else:
        return str(value)
    
def map_to_oracle_date(date_str: str) -> str:
    """
    Konvertiert ein Datum im Format 'YYYYMMDD' in das Oracle DATE-Format.

    :param date_str: Datum als String im Format "YYYYMMDD"
    :return: SQL-kompatibler String für Oracle TO_DATE
    """
    try:
        # Überprüfung: Muss 8 Zeichen lang sein und nur Ziffern enthalten
        if not (date_str.isdigit() and len(date_str) == 8):
            raise ValueError("Das Datum muss im Format 'YYYYMMDD' sein.")

        # Oracle SQL TO_DATE-Format
        return f"TO_DATE('{date_str}', 'YYYYMMDD')"

    except Exception as e:
        return f"Fehler: {str(e)}"
    
def map_to_date(date_str: str) -> datetime:
    """
    Konvertiert ein Datum im Format 'YYYYMMDD' in das datetime date-Format.

    :param date_str: Datum als String im Format "YYYYMMDD"
    :return: Entsprechender String im Format date(yyyy, (m)m, (d)d)
    """
    try:
        # Überprüfung: Muss 8 Zeichen lang sein und nur Ziffern enthalten
        if not (date_str.isdigit() and len(date_str) == 8):
            raise ValueError("Das Datum muss im Format 'YYYYMMDD' sein.")

        return datetime(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:]), 0, 0)

    except Exception as e:
        return f"Fehler: {str(e)}"

    
def map_to_oracle_timestamp(timestamp_str: str) -> str:
    """
    Konvertiert einen String im Format 'HH:MM:SS' in das Oracle TIMESTAMP-Format.

    :param timestamp_str: Zeitstempel als String im Format "HH:MM:SS"
    :return: SQL-kompatibler String für Oracle TO_TIMESTAMP
    """
    try:
        # Überprüfung: Muss 8 Zeichen lang sein
        if len(timestamp_str) != 8:
            raise ValueError("Der Zeitstempel muss im Format 'HH:MM:SS' sein.")
        
        # Wenn über 24 Stunden hinausgegangen wird, wird wieder bei 00:00:00 begonnen
        if int(timestamp_str[:2]) >= 24:
            timestamp_str = "00" + timestamp_str[2:]

        # Oracle SQL TO_TIMESTAMP-Format
        return f"TO_TIMESTAMP('{timestamp_str}', 'HH24:MI:SS')"

    except Exception as e:
        return f"Fehler: {str(e)}"
    
def map_to_datetime(timestamp_str: str) -> datetime:
    """
     Konvertiert einen String im Format 'HH:MM:SS' in das datetime-Format.

    :param timestamp_str: Zeitstempel als String im Format "HH:MM:SS"
    :return: Entsprechender String im Format datetime(yyyy, (m)m, (d)d, (h)h, (m)m, (s)s)
    """
    try:
        # Überprüfung: Muss 8 Zeichen lang sein
        if len(timestamp_str) != 8:
            raise ValueError("Der Zeitstempel muss im Format 'HH:MM:SS' sein.")

        # Wenn über 24 Stunden hinausgegangen wird, wird wieder bei 00:00:00 begonnen
        if int(timestamp_str[:2]) >= 24:
            timestamp_str = "00" + timestamp_str[2:]
        
        return datetime.strptime(timestamp_str, '%H:%M:%S')

    except Exception as e:
        return f"Fehler: {str(e)}"  


def get_minute_difference(first_point_in_time: str, second_point_in_time: str) -> int:
    """
    Berechnet die Differenz in Minuten zwischen zwei Uhrzeiten im Format 'HH:MM:SS'.

    :param time1: Erster Zeitstempel als String im Format "HH:MM:SS"
    :param time2: Zweiter Zeitstempel als String im Format "HH:MM:SS"
    :return: Differenz in Minuten (maximal 1440 Minuten)
    """
    try:
        # Überprüfung: Muss 8 Zeichen lang sein
        if len(first_point_in_time) != 8 or len(second_point_in_time) != 8:
            raise ValueError("Der Zeitstempel muss im Format 'HH:MM:SS' sein.")
        
        # Berechne die Differenz in Minuten
        first_point_in_time: datetime = datetime.strptime(first_point_in_time, '%H:%M:%S')
        second_point_in_time: datetime = datetime.strptime(second_point_in_time, '%H:%M:%S')
        if first_point_in_time > second_point_in_time:
            second_point_in_time = second_point_in_time.replace(day=second_point_in_time.day + 1)
        return (second_point_in_time - first_point_in_time).seconds // 60
    
    except Exception as e:
        return f"Fehler beim Ausrechnen eines Zeitunterschieds: {str(e)}"
    

def get_time_when_more_than_24_h(time: str) -> str:
    """
    Gibt bei Uhrzeiten im Format 'HH:MM:SS' die Zeit zurück, die über 24 Stunden hinausgeht,
    wenn die Uhrzeit mehr als 24 Stunden beträgt.

    :param time: Zeitstempel als String im Format "HH:MM:SS"
    """
    try:
        # Überprüfung: Muss 8 Zeichen lang sein
        if len(time) != 8:
            raise ValueError("Der Zeitstempel muss im Format 'HH:MM:SS' sein.")
        
        # Wenn über 24 Stunden hinausgegangen wird, wird wieder bei 00:00:00 begonnen
        if int(time[:2]) >= 24:
            hours = str(int(time[:2]) % 24)
            if len(hours) == 1:
                hours = "0" + hours
            time = hours + time[2:]
        return time
    
    except Exception as e:
        return f"Fehler beim Ausrechnen einer Zeit: {str(e)}"
    

def append_new_columns_and_get_used(for_table_name: str, from_table_columns: list[str], already_new_columns: list[str] = []) -> dict[str,list[str]]:
    """
    Fügt die neuen Spalten hinzu, die in der Datenbanktabelle noch nicht vorhanden sind.

    :param for_table_name: Name der Datenbanktabelle, für die die Spalten hinzugefügt werden sollen
    :param from_table_columns: Spalten der GTFS-Tabelle, die hinzugefügt werden sollen
    :param already_new_columns: Bereits hinzugefügte Spalten
    :return: Dict mit den hinzugefügten Spalten und den verwendeten Spalten
    """
    new_columns = []
    used_columns = []
    if for_table_name in column_names_map:
        for old_column in from_table_columns:
            if old_column in column_names_map[for_table_name]:
                used_columns.append(old_column)
                for new_column in column_names_map[for_table_name][old_column]:
                    new_columns.append(new_column)
    
    for already_new_column in already_new_columns:
        if already_new_column not in new_columns:
            new_columns.append(already_new_column)
    
    return {"new_columns": new_columns, "used_columns": used_columns}
    
