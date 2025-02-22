from datetime import datetime

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