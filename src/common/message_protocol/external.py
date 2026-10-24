class MsgType:
    ARTICLE = 1
    CLIENT_ID = 2
    ACK = 3
    END_OF_RECORDS = 4


UINT32_SIZE = 4


def _read_all(read, size):
    data = bytearray()
    while len(data) < size:
        chunk = read(size - len(data))
        if not chunk:
            raise EOFError(f"End of data after {len(data)} of {size} bytes")
        data += chunk
    return bytes(data)


def recv_all(connection, size):
    return _read_all(connection.recv, size)


def send_all(connection, data):
    view = memoryview(data)
    while len(view):
        sent = connection.send(view)
        if sent == 0:
            raise ConnectionError("Connection closed while sending a message")
        view = view[sent:]


def _read_uint32(read):
    return int.from_bytes(_read_all(read, UINT32_SIZE), byteorder="big")


def _recv_uint32(connection):
    return _read_uint32(connection.recv)


def _serialize_uint32(value):
    return value.to_bytes(UINT32_SIZE, byteorder="big")


def _validate_id(client_id):
    if type(client_id) is not int or client_id <= 0:
        raise ValueError("Client ID must be a positive integer")


def _recv_client_id(connection):
    client_id = _recv_uint32(connection)
    _validate_id(client_id)
    return client_id


def _serialize_client_id(client_id):
    _validate_id(client_id)
    return _serialize_uint32(client_id)


def _read_bytes(read):
    size = _read_uint32(read)
    return _read_all(read, size)


def _serialize_bytes(value):
    if not isinstance(value, bytes):
        raise ValueError("Expected bytes")
    return _serialize_uint32(len(value)) + value


def _read_text(read):
    return _read_bytes(read).decode("utf-8")


def _serialize_text(value):
    return _serialize_bytes(value.encode("utf-8"))


def _read_article(read):
    field_count = _read_uint32(read)
    article = {}
    for _ in range(field_count):
        name = _read_text(read)
        value = _read_text(read)
        if name in article:
            raise ValueError("Repeated article field")
        article[name] = value
    return article


def _recv_article(connection):
    return _read_article(connection.recv)


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
    MsgType.CLIENT_ID: _recv_client_id,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
}

SEND_MSG_HANDLERS = {
    MsgType.ARTICLE: _serialize_article,
    MsgType.CLIENT_ID: _serialize_client_id,
    MsgType.ACK: _serialize_empty,
    MsgType.END_OF_RECORDS: _serialize_empty,
}


def recv_msg(connection):
    message_type = _recv_uint32(connection)
    if message_type not in RECV_MSG_HANDLERS:
        raise ValueError("Unknown message type")
    message = RECV_MSG_HANDLERS[message_type](connection)
    return message_type, message


def serialize_msg(message_type, *args):
    if message_type not in SEND_MSG_HANDLERS:
        raise ValueError("Unknown message type")
    payload = SEND_MSG_HANDLERS[message_type](*args)
    return _serialize_uint32(message_type) + payload


def send_msg(connection, message_type, *args):
    send_all(connection, serialize_msg(message_type, *args))
