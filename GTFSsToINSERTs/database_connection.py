import cx_Oracle

# Verbindung zur Oracle-Datenbank auf localhost:1521 herstellen
def get_db_connection():
    dsn_tns = cx_Oracle.makedsn("localhost", "1521", service_name="XEPDB1")
    conn = cx_Oracle.connect(user="C##Benutzer", password="saqvizsanqoxsizRe9", dsn=dsn_tns)
    return conn