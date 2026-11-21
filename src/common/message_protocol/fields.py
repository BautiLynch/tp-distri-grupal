"""Codificacion TLV de campos compartida por los protocolos interno y externo.

Cada campo se codifica como [tipo: 1 byte][largo: uint32][contenido].
Las listas y tuplas contienen sus elementos codificados de la misma forma.
"""

TYPE_INT = 1
TYPE_TEXT = 2
TYPE_BYTES = 3
TYPE_LIST = 4
TYPE_TUPLE = 5

UINT32_SIZE = 4
LENGTH_SIZE = UINT32_SIZE
FIELD_HEADER_SIZE = 1 + LENGTH_SIZE


def _encode(value):
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
        field_type = TYPE_LIST if isinstance(value, list) else TYPE_TUPLE
        content = b"".join(_encode(item) for item in value)

    else:
        raise TypeError(f"Unsupported field type: {type(value).__name__}")

    return (
        bytes((field_type,))
        + len(content).to_bytes(LENGTH_SIZE, "big")
        + content
    )


def _decode(data):
    fields = []
    offset = 0

    while offset < len(data):
        header_end = offset + FIELD_HEADER_SIZE

        if header_end > len(data):
            raise ValueError("Incomplete field header")

        field_type = data[offset]
        size = int.from_bytes(data[offset + 1:header_end], "big")
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
            value = _decode(content)

        elif field_type == TYPE_TUPLE:
            value = tuple(_decode(content))

        else:
            raise ValueError(f"Unknown field type: {field_type}")

        fields.append(value)
        offset = content_end

    return fields


def encode_fields(*fields):
    return b"".join(_encode(field) for field in fields)


def decode_fields(payload):
    return tuple(_decode(payload))
