import json

from . import external


class MsgType:
    RESULT = 1
    QUERY_END = 2
    ARTICLE = 3
    END_OF_RECORDS = 4


QUERY_COUNT = 5
INPUT_QUERY_ID = 0
UINT32_SIZE = 4
MESSAGE_TYPE_OFFSET = 0
CLIENT_ID_OFFSET = MESSAGE_TYPE_OFFSET + UINT32_SIZE
QUERY_ID_OFFSET = CLIENT_ID_OFFSET + UINT32_SIZE
PAYLOAD_SIZE_OFFSET = QUERY_ID_OFFSET + UINT32_SIZE
HEADER_SIZE = PAYLOAD_SIZE_OFFSET + UINT32_SIZE


def _serialize(message_type, client_id, query_id, payload):
    return (
        message_type.to_bytes(UINT32_SIZE, byteorder="big")
        + client_id.to_bytes(UINT32_SIZE, byteorder="big")
        + query_id.to_bytes(UINT32_SIZE, byteorder="big")
        + len(payload).to_bytes(UINT32_SIZE, byteorder="big")
        + payload
    )


def serialize_result(client_id, query_id, external_type, *fields):
    message = external.serialize_msg(external_type, *fields)
    return serialize_result_bytes(client_id, query_id, message)


def serialize_result_bytes(client_id, query_id, payload):
    """Keep the query's payload opaque; the handler adds its external envelope."""
    if not isinstance(payload, bytes):
        raise ValueError("Result payload must be bytes")
    return _serialize(MsgType.RESULT, client_id, query_id, payload)


def serialize_query_end(client_id, query_id):
    """The whole query has finished, including every replica and its results."""
    return _serialize(MsgType.QUERY_END, client_id, query_id, b"")


def _deserialize(message):
    if len(message) < HEADER_SIZE:
        raise ValueError("Incomplete internal header")
    message_type = int.from_bytes(message[MESSAGE_TYPE_OFFSET:CLIENT_ID_OFFSET], byteorder="big")
    client_id = int.from_bytes(message[CLIENT_ID_OFFSET:QUERY_ID_OFFSET], byteorder="big")
    query_id = int.from_bytes(message[QUERY_ID_OFFSET:PAYLOAD_SIZE_OFFSET], byteorder="big")
    size = int.from_bytes(message[PAYLOAD_SIZE_OFFSET:HEADER_SIZE], byteorder="big")
    if size != len(message) - HEADER_SIZE:
        raise ValueError("Internal payload length does not match the message")
    payload = message[HEADER_SIZE:]
    return message_type, client_id, query_id, payload


def deserialize_result(message):
    return _deserialize(message)


def serialize_article(client_id, article):
    if not isinstance(article, dict):
        raise ValueError("An article must be a dictionary")
    fields = {
        name: json.dumps(value, ensure_ascii=False, allow_nan=False)
        for name, value in article.items()
    }
    payload = external._serialize_article(fields)
    return _serialize(MsgType.ARTICLE, client_id, INPUT_QUERY_ID, payload)


def serialize_end_of_records(client_id):
    return _serialize(MsgType.END_OF_RECORDS, client_id, INPUT_QUERY_ID, b"")


def _deserialize_text(payload, offset):
    size_end = offset + UINT32_SIZE
    if size_end > len(payload):
        raise ValueError("Incomplete article field length")
    size = int.from_bytes(payload[offset:size_end], byteorder="big")
    value_end = size_end + size
    if value_end > len(payload):
        raise ValueError("Incomplete article field")
    value = payload[size_end:value_end].decode("utf-8")
    return value, value_end


def deserialize_input(message):
    message_type, client_id, _, payload = _deserialize(message)
    if message_type == MsgType.END_OF_RECORDS:
        return message_type, client_id, None
    if message_type != MsgType.ARTICLE:
        raise ValueError("Unknown internal input type")
    if len(payload) < UINT32_SIZE:
        raise ValueError("Incomplete article field count")
    field_count = int.from_bytes(payload[:UINT32_SIZE], byteorder="big")
    offset = UINT32_SIZE
    article = {}
    for _ in range(field_count):
        name, offset = _deserialize_text(payload, offset)
        value, offset = _deserialize_text(payload, offset)
        if name in article:
            raise ValueError("Repeated article field")
        article[name] = json.loads(value)
    if offset != len(payload):
        raise ValueError("Unexpected bytes after the article")
    return message_type, client_id, article
