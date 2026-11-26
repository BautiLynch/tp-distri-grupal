from enum import IntEnum, auto

from .fields import UINT32_SIZE, decode_fields, encode_fields


class MsgType(IntEnum):
    ARTICLE = 1
    CLIENT_ID = auto()
    ACK = auto()
    END_OF_RECORDS = auto()
    HANDLER_ADDRESS = auto()
    RESULT = auto()


HEADER_SIZE = 2 * UINT32_SIZE


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


def serialize_msg(message_type, *fields):
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
    return MsgType(message_type), fields


def send_msg(connection, message_type, *fields):
    send_all(connection, serialize_msg(message_type, *fields))
