# Enum, der die Werte INTEGER, DATE, TIME, TEXT und FLOAT annehmen kann
from enum import Enum

class DatatypeEnum(Enum):
    INTEGER = 1
    DATE = 2
    TIME = 3
    TEXT = 4
    FLOAT = 5