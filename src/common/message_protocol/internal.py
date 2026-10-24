from . import external


class MsgType:
    RESULT = 1
    QUERY_END = 2


QUERY_COUNT = 5
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
    return _serialize(MsgType.RESULT, client_id, query_id, message)


def serialize_query_end(client_id, query_id):
    """The whole query has finished, including every replica and its results."""
    return _serialize(MsgType.QUERY_END, client_id, query_id, b"")


def deserialize_result(message):
    if len(message) < HEADER_SIZE:
        raise ValueError("Incomplete internal result header")
    message_type = int.from_bytes(message[MESSAGE_TYPE_OFFSET:CLIENT_ID_OFFSET], byteorder="big")
    client_id = int.from_bytes(message[CLIENT_ID_OFFSET:QUERY_ID_OFFSET], byteorder="big")
    query_id = int.from_bytes(message[QUERY_ID_OFFSET:PAYLOAD_SIZE_OFFSET], byteorder="big")
    size = int.from_bytes(message[PAYLOAD_SIZE_OFFSET:HEADER_SIZE], byteorder="big")
    if size != len(message) - HEADER_SIZE:
        raise ValueError("Internal payload length does not match the message")
    payload = message[HEADER_SIZE:]
    return message_type, client_id, query_id, payload
