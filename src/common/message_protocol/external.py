class MsgType:
    ARTICLE = 1
    CLIENT_ID = 2
    ACK = 3
    END_OF_RECORDS = 4


UINT32_SIZE = 4


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


def _recv_uint32(connection):
    return int.from_bytes(recv_all(connection, UINT32_SIZE), byteorder="big")


def _serialize_uint32(value):
    if type(value) is not int or not 0 <= value <= 0xFFFFFFFF:
        raise ValueError("Expected an unsigned 32-bit integer")
    return value.to_bytes(UINT32_SIZE, byteorder="big")


def _recv_bytes(connection):
    size = _recv_uint32(connection)
    return recv_all(connection, size)


def _serialize_bytes(value):
    if not isinstance(value, bytes):
        raise ValueError("Expected bytes")
    return _serialize_uint32(len(value)) + value


def _recv_text(connection):
    return _recv_bytes(connection).decode("utf-8")


def _serialize_text(value):
    return _serialize_bytes(value.encode("utf-8"))


def _recv_article(connection):
    field_count = _recv_uint32(connection)
    article = {}
    for _ in range(field_count):
        name = _recv_text(connection)
        value = _recv_text(connection)
        if name in article:
            raise ValueError("Repeated article field")
        article[name] = value
    return article


def _serialize_article(article):
    if not isinstance(article, dict):
        raise ValueError("An article must be a dictionary")
    fields = [_serialize_uint32(len(article))]
    for name, value in article.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError("Article field names and values must be strings")
        fields.extend((_serialize_text(name), _serialize_text(value)))
    return b"".join(fields)


def _recv_empty(connection):
    return None


def _serialize_empty():
    return b""


RECV_MSG_HANDLERS = {
    MsgType.ARTICLE: _recv_article,
    MsgType.CLIENT_ID: _recv_uint32,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
}

SEND_MSG_HANDLERS = {
    MsgType.ARTICLE: _serialize_article,
    MsgType.CLIENT_ID: _serialize_uint32,
    MsgType.ACK: _serialize_empty,
    MsgType.END_OF_RECORDS: _serialize_empty,
}


def _validate_id(message_type, value):
    if message_type == MsgType.CLIENT_ID and value == 0:
        raise ValueError("Client ID must be positive")


def recv_msg(connection):
    message_type = _recv_uint32(connection)
    if message_type not in RECV_MSG_HANDLERS:
        raise ValueError("Unknown message type")
    message = RECV_MSG_HANDLERS[message_type](connection)
    _validate_id(message_type, message)
    return message_type, message


def send_msg(connection, message_type, *args):
    if message_type not in SEND_MSG_HANDLERS:
        raise ValueError("Unknown message type")
    payload = SEND_MSG_HANDLERS[message_type](*args)
    if args:
        _validate_id(message_type, args[0])
    send_all(connection, _serialize_uint32(message_type) + payload)
