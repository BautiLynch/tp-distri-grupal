from enum import IntEnum, auto

class MsgType(IntEnum):
    Q1_RESULT = 1
    Q2_RESULT = auto()
    Q3_RESULT = auto()
    Q4_RESULT = auto()
    Q5_RESULT = auto()
    QUERY_END = auto()
    WORD_LANGUAGE = auto()
    WORD_AMOUNT = auto()
    WORD_YEAR_AMOUNT = auto()
    END_OF_RECORDS = auto()
    SPLITTER_ARTICLE = auto()
    GET_IMAGE = auto()


QUERY_COUNT = 5
UINT32_SIZE = 4
MESSAGE_TYPE_OFFSET = 0
CLIENT_ID_OFFSET = MESSAGE_TYPE_OFFSET + UINT32_SIZE
PAYLOAD_SIZE_OFFSET = CLIENT_ID_OFFSET + UINT32_SIZE
HEADER_SIZE = PAYLOAD_SIZE_OFFSET + UINT32_SIZE


def _serialize(message_type, client_id, payload):
    return (
        message_type.to_bytes(UINT32_SIZE, byteorder="big") +
        client_id.to_bytes(UINT32_SIZE, byteorder="big") +
        len(payload).to_bytes(UINT32_SIZE, byteorder="big") +
        _serialize_payload(payload)
    )

# HEADER (type, client_id, payload_size) + PAYLOAD
# PAYLOAD: (tamaño de cada campo + campo) * cantidad de campos
def _serialize_payload(*fields):
    parts = []
    for value in fields:
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif not isinstance(value, (bytes, bytearray, memoryview)):
            raise ValueError("Record fields must be str or bytes")
        
        parts.append(len(value).to_bytes(UINT32_SIZE, "big"))
        parts.append(value)

    return b"".join(parts)

def _deserialize(message):
    if len(message) < HEADER_SIZE:
        raise ValueError("Incomplete internal header")
    
    message_type = int.from_bytes(message[MESSAGE_TYPE_OFFSET:CLIENT_ID_OFFSET], byteorder="big")
    client_id = int.from_bytes(message[CLIENT_ID_OFFSET:PAYLOAD_SIZE_OFFSET], byteorder="big")
    size = int.from_bytes(message[PAYLOAD_SIZE_OFFSET:HEADER_SIZE], byteorder="big")

    if size != len(message) - HEADER_SIZE:
        raise ValueError("Internal payload length does not match the message")
    
    payload = message[HEADER_SIZE:]
    return message_type, client_id, payload


def _serialize_texts(fields):
    parts = []
    for value in fields:
        if not isinstance(value, str):
            raise ValueError("Record fields must be strings")
        encoded = value.encode("utf-8")
        parts.extend((len(encoded).to_bytes(UINT32_SIZE, "big"), encoded))
    return b"".join(parts)


def _serialize_filter_article(name, url, language, date_modified, image_url, sections):
    payload = _serialize_texts((name, url, language, date_modified, image_url))
    payload += len(sections).to_bytes(UINT32_SIZE, "big")
    return payload + _serialize_texts(sections)


def _serialize_splitter_article(url, language, date_modified, abstract):
    return _serialize_texts((url, language, date_modified, abstract))


def _serialize_word(url, language, date_modified, word, amount): # TODO: Revisar xq cambia el ms segun la query
    payload = _serialize_texts((url, language, date_modified, word))
    return payload + amount.to_bytes(UINT32_SIZE, "big")


def _serialize_query1_result(url, date_modified):
    return _serialize_texts((url, date_modified))

def _serialize_query2_result(name, url, image_bytes):
    payload = _serialize_texts((name, url))
    payload += len(image_bytes).to_bytes(UINT32_SIZE, "big")
    return payload + image_bytes

def _serialize_query4_result(client_id, top_list):
    payload = client_id.to_bytes(UINT32_SIZE, "big")
    payload += len(top_list).to_bytes(UINT32_SIZE, "big")
    for word, amount in top_list:
        payload += _serialize_texts((word,))
        payload += amount.to_bytes(UINT32_SIZE, "big")
    return payload

def _serialize_query3_result(word, client_id):
    return _serialize_texts((word, str(client_id)))

def _serialize_empty():
    return b""


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


def _deserialize_texts(payload, count, offset=0):
    fields = []
    for _ in range(count):
        value, offset = _deserialize_text(payload, offset)
        fields.append(value)
    return tuple(fields), offset


def _deserialize_uint32(payload, offset):
    end = offset + UINT32_SIZE
    if end > len(payload):
        raise ValueError("Incomplete record integer")
    return int.from_bytes(payload[offset:end], "big"), end


def _check_end(payload, offset):
    if offset != len(payload):
        raise ValueError("Unexpected bytes after record")


def _deserialize_filter_article(payload):
    fields, offset = _deserialize_texts(payload, 5)
    section_count, offset = _deserialize_uint32(payload, offset)
    sections, offset = _deserialize_texts(payload, section_count, offset)
    _check_end(payload, offset)
    return fields + (sections,)


def _deserialize_splitter_article(payload):
    fields, offset = _deserialize_texts(payload, 4)
    _check_end(payload, offset)
    return fields


def _deserialize_word(payload):
    fields, offset = _deserialize_texts(payload, 4)
    amount, offset = _deserialize_uint32(payload, offset)
    _check_end(payload, offset)
    return fields + (amount,)


def _deserialize_query1_result(payload):
    fields, offset = _deserialize_texts(payload, 2)
    _check_end(payload, offset)
    return fields

def _deserialize_query2_result(payload):
    fields, offset = _deserialize_texts(payload, 2)
    image_size, offset = _deserialize_uint32(payload, offset)
    if offset + image_size > len(payload):
        raise ValueError("Incomplete image bytes")
    image_bytes = payload[offset:offset + image_size]
    offset += image_size
    _check_end(payload, offset)
    return fields + (image_bytes,)

def _deserialize_query4_result(payload):
    client_id, offset = _deserialize_uint32(payload, 0)
    top_list_count, offset = _deserialize_uint32(payload, offset)
    top_list = []
    for _ in range(top_list_count):
        word, offset = _deserialize_text(payload, offset)
        amount, offset = _deserialize_uint32(payload, offset)
        top_list.append((word, amount))
    _check_end(payload, offset)
    return (client_id, top_list)

def _deserialize_empty(payload):
    _check_end(payload, 0)
    return ()


RECV_MSG_HANDLERS = {
    MsgType.FILTER_ARTICLE: _deserialize_filter_article,
    MsgType.SPLITTER_ARTICLE: _deserialize_splitter_article,
    MsgType.WORD: _deserialize_word,
    MsgType.Q1_RESULT: _deserialize_query1_result,
    MsgType.Q2_RESULT: _deserialize_query2_result,
    MsgType.Q3_RESULT: _deserialize_query1_result,
    MsgType.Q4_RESULT: _deserialize_query4_result,
    MsgType.END_OF_RECORDS: _deserialize_empty,
    MsgType.QUERY_END: _deserialize_empty,
}

SEND_MSG_HANDLERS = {
    MsgType.FILTER_ARTICLE: _serialize_filter_article,
    MsgType.SPLITTER_ARTICLE: _serialize_splitter_article,
    MsgType.WORD: _serialize_word,
    MsgType.Q1_RESULT: _serialize_query1_result,
    MsgType.Q2_RESULT: _serialize_query2_result,
    MsgType.Q3_RESULT: _serialize_query1_result,
    MsgType.Q4_RESULT: _serialize_query4_result,
    MsgType.END_OF_RECORDS: _serialize_empty,
    MsgType.QUERY_END: _serialize_empty,
}


def serialize_payload(message_type, *fields):
    if message_type not in SEND_MSG_HANDLERS:
        raise ValueError("Unknown internal message type")
    return SEND_MSG_HANDLERS[message_type](*fields)


def deserialize_payload(message_type, payload):
    if message_type not in RECV_MSG_HANDLERS:
        raise ValueError("Unknown internal message type")
    return RECV_MSG_HANDLERS[message_type](payload)


def serialize_msg(message_type, client_id, *fields):
    payload = serialize_payload(message_type, *fields)
    return _serialize(message_type, client_id, payload)


def deserialize_msg(message):
    message_type, client_id, payload = _deserialize(message)
    fields = deserialize_payload(message_type, payload)
    return message_type, client_id, fields
