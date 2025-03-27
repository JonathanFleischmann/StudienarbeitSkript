import cx_Oracle
from neo4j import GraphDatabase

# Verbindung zur Oracle-Datenbank auf localhost:1521 herstellen
def get_orcle_db_connection(host: str, port: int, service: str, username: str, password: str)-> cx_Oracle.Connection:
    """
    Stellt eine Verbindung zu einer Oracle-Datenbank her.
    
    :param host: Hostname der Oracle-Datenbank
    :param port: Port der Oracle-Datenbank
    :param service: Service-Name der Oracle-Datenbank
    :param username: Benutzername für die Authentifizierung
    :param password: Passwort für die Authentifizierung
    :return: Ein cx_Oracle.Connection-Objekt
    """
    dsn_tns = cx_Oracle.makedsn(host, port, service_name=service)
    conn = cx_Oracle.connect(username, password, dsn=dsn_tns)
    return conn

# Verbindung zur Neo4j-Datenbank herstellen
def get_neo4j_db_connection(uri: str, username: str, password: str) -> GraphDatabase:
    """
    Stellt eine Verbindung zu einer Neo4j-Datenbank her.

    :param uri: URI der Neo4j-Datenbank (z. B. "bolt://localhost:7687")
    :param username: Benutzername für die Authentifizierung
    :param password: Passwort für die Authentifizierung
    :return: Ein GraphDatabase.Driver-Objekt
    """
    driver = GraphDatabase.driver(uri, auth=(username, password))
    return driver