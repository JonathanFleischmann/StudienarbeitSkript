import cx_Oracle

# Verbindung zur Oracle-Datenbank auf localhost:1521 herstellen
def get_orcle_db_connection(host: str, port: int, service: str, username: str, password: str)-> cx_Oracle.Connection:
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=service)
    conn = cx_Oracle.connect(username, password, dsn=dsn_tns)
    return conn