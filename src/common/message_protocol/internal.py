from enum import IntEnum, auto

from .fields import UINT32_SIZE, decode_fields, encode_fields


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


MESSAGE_TYPE_OFFSET = 0
CLIENT_ID_OFFSET = MESSAGE_TYPE_OFFSET + UINT32_SIZE
PAYLOAD_SIZE_OFFSET = CLIENT_ID_OFFSET + UINT32_SIZE
HEADER_SIZE = PAYLOAD_SIZE_OFFSET + UINT32_SIZE


def serialize_msg(message_type, client_id, *fields):
    # Valida que el tipo de mensaje esté definido.
    MsgType(message_type)
    payload = encode_fields(*fields)
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

    MsgType(message_type)
    fields = decode_fields(message[HEADER_SIZE:])
    return message_type, client_id, fields
