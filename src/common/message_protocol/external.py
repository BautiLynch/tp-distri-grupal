"""Protocolo entre el cliente y el sistema (gateway y handler).

Cada mensaje es [tipo: uint32][largo del payload: uint32][payload], donde el
payload son los campos codificados con el formato TLV de `fields`.
`recv_msg` siempre devuelve (tipo, tupla de campos).
"""

from enum import IntEnum, auto

from .fields import UINT32_SIZE, decode_fields, encode_fields


class MsgType(IntEnum):
    ARTICLE = 1
    CLIENT_ID = auto()
    ACK = auto()
    END_OF_RECORDS = auto()
    HANDLER_ADDRESS = auto()
    RESULT = auto()


class QueryId(IntEnum):
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    Q5 = 5


HEADER_SIZE = 2 * UINT32_SIZE

# Cantidad de campos de cada resultado, sin contar el query ID.
RESULT_FIELD_COUNTS = {
    QueryId.Q1: 2,  # url, date_modified
    QueryId.Q2: 3,  # name, url, image
    QueryId.Q3: 1,  # word
    QueryId.Q4: 1,  # [(word, amount), ...]
    QueryId.Q5: 2,  # url, amount
}


def recv_all(connection, size):
    data = bytearray()
    while len(data) < size:
        chunk = connection.recv(size - len(data))
        if not chunk:
            raise EOFError(f"Connection closed after {len(data)} of {size} bytes")
        data += chunk
    return bytes(data)


def send_all(connection, data):
    view = memoryview(data)
    while len(view):
        sent = connection.send(view)
        if sent == 0:
            raise ConnectionError("Connection closed while sending a message")
        view = view[sent:]


def _expect_fields(fields, count, name):
    if len(fields) != count:
        raise ValueError(f"{name} expects {count} fields, got {len(fields)}")


def _validate_article(*fields):
    _expect_fields(fields, 1, "ARTICLE")
    if not isinstance(fields[0], bytes):
        raise ValueError("Article must be bytes")


def _validate_client_id(*fields):
    _expect_fields(fields, 1, "CLIENT_ID")
    client_id, = fields
    if type(client_id) is not int or client_id <= 0:
        raise ValueError("Client ID must be a positive integer")


def _validate_empty(*fields):
    _expect_fields(fields, 0, "Message")


def _validate_handler_address(*fields):
    _expect_fields(fields, 2, "HANDLER_ADDRESS")
    host, port = fields
    if not isinstance(host, str) or not host.strip():
        raise ValueError("Handler host must not be empty")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("Handler port must be between 1 and 65535")


def _validate_result(*fields):
    if not fields:
        raise ValueError("RESULT requires a query ID")
    query_id, *values = fields
    if query_id not in RESULT_FIELD_COUNTS:
        raise ValueError(f"Unknown query ID: {query_id}")
    _expect_fields(values, RESULT_FIELD_COUNTS[query_id], f"Query {query_id}")


VALIDATORS = {
    MsgType.ARTICLE: _validate_article,
    MsgType.CLIENT_ID: _validate_client_id,
    MsgType.ACK: _validate_empty,
    MsgType.END_OF_RECORDS: _validate_empty,
    MsgType.HANDLER_ADDRESS: _validate_handler_address,
    MsgType.RESULT: _validate_result,
}


def _validate(message_type, fields):
    if message_type not in VALIDATORS:
        raise ValueError(f"Unknown message type: {message_type}")
    VALIDATORS[message_type](*fields)

def serialize_msg(message_type, *fields):
    _validate(message_type, fields)
    payload = encode_fields(*fields)
    return (
        int(message_type).to_bytes(UINT32_SIZE, "big")
        + len(payload).to_bytes(UINT32_SIZE, "big")
        + payload
    )


def recv_msg(connection):
    header = recv_all(connection, HEADER_SIZE)
    message_type = int.from_bytes(header[:UINT32_SIZE], "big")
    size = int.from_bytes(header[UINT32_SIZE:], "big")
    fields = decode_fields(recv_all(connection, size))
    _validate(message_type, fields)
    return MsgType(message_type), fields


def send_msg(connection, message_type, *fields):
    send_all(connection, serialize_msg(message_type, *fields))
