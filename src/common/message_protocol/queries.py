from enum import IntEnum, auto


class QueryId(IntEnum):
    """Identificador de cada query, compartido por el protocolo interno y el externo."""

    Q1 = 1
    Q2 = auto()
    Q3 = auto()
    Q4 = auto()
    Q5 = auto()
