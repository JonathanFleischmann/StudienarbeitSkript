
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
