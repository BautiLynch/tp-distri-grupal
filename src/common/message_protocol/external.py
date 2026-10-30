class MsgType:
    ARTICLE = 1
    CLIENT_ID = 2
    ACK = 3
    END_OF_RECORDS = 4
    ARTICLE_LINE = 5
    HANDLER_ADDRESS = 6
    RESULT = 7


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


def _recv_handler_address(connection):
    host = _recv_text(connection)
    port = _recv_uint32(connection)
    _validate_address(host, port)
    return host, port


def _validate_address(host, port):
    if not isinstance(host, str) or not host.strip():
        raise ValueError("Handler host must not be empty")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("Handler port must be between 1 and 65535")


def _serialize_handler_address(host, port):
    _validate_address(host, port)
    return _serialize_text(host) + _serialize_uint32(port)


def _recv_result(connection):
    query_id = _recv_uint32(connection)
    if query_id == 0:
        raise ValueError("Query ID must be positive")
    return query_id, _recv_bytes(connection)


def _serialize_result(query_id, payload):
    if type(query_id) is not int or query_id <= 0:
        raise ValueError("Query ID must be positive")
    return _serialize_uint32(query_id) + _serialize_bytes(payload)


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
    MsgType.CLIENT_ID: _recv_client_id,
    MsgType.ACK: _recv_empty,
    MsgType.END_OF_RECORDS: _recv_empty,
    MsgType.ARTICLE_LINE: _recv_bytes,
    MsgType.HANDLER_ADDRESS: _recv_handler_address,
    MsgType.RESULT: _recv_result,
}

SEND_MSG_HANDLERS = {
    MsgType.ARTICLE: _serialize_article,
    MsgType.CLIENT_ID: _serialize_client_id,
    MsgType.ACK: _serialize_empty,
    MsgType.END_OF_RECORDS: _serialize_empty,
    MsgType.ARTICLE_LINE: _serialize_bytes,
    MsgType.HANDLER_ADDRESS: _serialize_handler_address,
    MsgType.RESULT: _serialize_result,
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
