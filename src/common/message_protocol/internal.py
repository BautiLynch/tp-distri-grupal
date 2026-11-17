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
    FILTER_ARTICLE = auto()
    SPLIT_WORD = auto()
    BASELINE_END = auto()
    WORD_BATCH = auto()
    HEARTBEAT = auto()


TYPE_INT = 1
TYPE_TEXT = 2
TYPE_BYTES = 3
TYPE_LIST = 4
TYPE_TUPLE = 5

UINT32_SIZE = 4
LENGTH_SIZE = UINT32_SIZE
FIELD_HEADER_SIZE = 1 + LENGTH_SIZE

MESSAGE_TYPE_OFFSET = 0
CLIENT_ID_OFFSET = MESSAGE_TYPE_OFFSET + UINT32_SIZE
PAYLOAD_SIZE_OFFSET = CLIENT_ID_OFFSET + UINT32_SIZE
HEADER_SIZE = PAYLOAD_SIZE_OFFSET + UINT32_SIZE


def serialize_payload(message_type, *fields):
    # Valida que el tipo de mensaje esté definido.
    MsgType(message_type)

    def encode(value):
        if isinstance(value, int) and not isinstance(value, bool):
            field_type = TYPE_INT
            content = value.to_bytes(UINT32_SIZE, "big")

        elif isinstance(value, str):
            field_type = TYPE_TEXT
            content = value.encode("utf-8")

        elif isinstance(value, bytes):
            field_type = TYPE_BYTES
            content = value

        elif isinstance(value, (list, tuple)):
            field_type = (
                TYPE_LIST if isinstance(value, list) else TYPE_TUPLE
            )
            content = b"".join(encode(item) for item in value)

        else:
            raise TypeError(
                f"Unsupported field type: {type(value).__name__}"
            )

        return (
            bytes((field_type,))
            + len(content).to_bytes(LENGTH_SIZE, "big")
            + content
        )

    return b"".join(encode(field) for field in fields)


def deserialize_payload(message_type, payload):
    MsgType(message_type)

    def decode_fields(data):
        fields = []
        offset = 0

        while offset < len(data):
            header_end = offset + FIELD_HEADER_SIZE

            if header_end > len(data):
                raise ValueError("Incomplete field header")

            field_type = data[offset]
            size = int.from_bytes(
                data[offset + 1:header_end], "big"
            )
            content_end = header_end + size

            if content_end > len(data):
                raise ValueError("Incomplete field content")

            content = data[header_end:content_end]

            if field_type == TYPE_INT:
                if size != UINT32_SIZE:
                    raise ValueError("Invalid integer length")
                value = int.from_bytes(content, "big")

            elif field_type == TYPE_TEXT:
                value = content.decode("utf-8")

            elif field_type == TYPE_BYTES:
                value = content

            elif field_type == TYPE_LIST:
                value = decode_fields(content)

            elif field_type == TYPE_TUPLE:
                value = tuple(decode_fields(content))

            else:
                raise ValueError(
                    f"Unknown field type: {field_type}"
                )

            fields.append(value)
            offset = content_end

        return fields

    return tuple(decode_fields(payload))


def serialize_msg(message_type, client_id, *fields):
    payload = serialize_payload(message_type, *fields)
    return (
        int(message_type).to_bytes(UINT32_SIZE, "big")
        + client_id.to_bytes(UINT32_SIZE, "big")
        + len(payload).to_bytes(UINT32_SIZE, "big")
        + payload
    )


def deserialize_msg(message):
    if len(message) < HEADER_SIZE:
        raise ValueError("Incomplete internal header")

    message_type = int.from_bytes(
        message[MESSAGE_TYPE_OFFSET:CLIENT_ID_OFFSET], "big"
    )
    client_id = int.from_bytes(
        message[CLIENT_ID_OFFSET:PAYLOAD_SIZE_OFFSET], "big"
    )
    size = int.from_bytes(
        message[PAYLOAD_SIZE_OFFSET:HEADER_SIZE], "big"
    )

    if size != len(message) - HEADER_SIZE:
        raise ValueError(
            "Internal payload length does not match the message"
        )
    
    payload = message[HEADER_SIZE:]

    fields = deserialize_payload(message_type, payload)
    return message_type, client_id, fields