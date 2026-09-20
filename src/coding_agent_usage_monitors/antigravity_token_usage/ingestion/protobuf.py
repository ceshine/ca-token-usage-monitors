"""Low-level Protocol Buffers wire-format reader."""

from __future__ import annotations

from dataclasses import dataclass

from .errors import ProtobufParseError


@dataclass(frozen=True)
class ProtobufField:
    """A decoded field from a Protocol Buffers wire-format payload."""

    field_number: int
    wire_type: int
    value: int | bytes


def read_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Read a LEB128 varint integer from data starting at offset.

    Args:
        data (bytes): Raw protobuf byte stream.
        offset (int): Starting byte offset.

    Returns:
        tuple[int, int]: Decoded varint integer and the new byte offset.

    Raises:
        ProtobufParseError: If varint is unterminated, truncated, or exceeds 10 bytes.
    """
    res = 0
    shift = 0
    idx = offset
    for _ in range(10):
        if idx >= len(data):
            raise ProtobufParseError("Truncated varint in payload", byte_offset=offset)
        b = data[idx]
        idx += 1
        res |= (b & 0x7F) << shift
        if not (b & 0x80):
            return res, idx
        shift += 7
    raise ProtobufParseError("Varint overflow (exceeds 10 bytes)", byte_offset=offset)


def parse_protobuf(data: bytes) -> list[ProtobufField]:
    """Parse raw protobuf byte data into a list of ProtobufField instances.

    Args:
        data (bytes): Wire-format protobuf binary blob.

    Returns:
        list[ProtobufField]: Parsed fields in original source order.

    Raises:
        ProtobufParseError: On truncated data, invalid wire type, or corrupt tags.
    """
    fields: list[ProtobufField] = []
    idx = 0
    length = len(data)
    while idx < length:
        tag_offset = idx
        tag, idx = read_varint(data, idx)
        wire_type = tag & 0x07
        field_number = tag >> 3

        if field_number == 0:
            raise ProtobufParseError("Invalid field number 0", byte_offset=tag_offset)

        if wire_type == 0:
            varint_val, idx = read_varint(data, idx)
            fields.append(ProtobufField(field_number=field_number, wire_type=0, value=varint_val))
        elif wire_type == 1:
            if idx + 8 > length:
                raise ProtobufParseError(
                    f"Truncated fixed64 for field {field_number}",
                    field_path=str(field_number),
                    byte_offset=idx,
                )
            val = data[idx : idx + 8]
            idx += 8
            fields.append(ProtobufField(field_number=field_number, wire_type=1, value=val))
        elif wire_type == 2:
            payload_len, idx = read_varint(data, idx)
            if idx + payload_len > length:
                raise ProtobufParseError(
                    f"Length prefix {payload_len} extends beyond blob boundary for field {field_number}",
                    field_path=str(field_number),
                    byte_offset=idx,
                )
            payload = data[idx : idx + payload_len]
            idx += payload_len
            fields.append(ProtobufField(field_number=field_number, wire_type=2, value=payload))
        elif wire_type == 5:
            if idx + 4 > length:
                raise ProtobufParseError(
                    f"Truncated fixed32 for field {field_number}",
                    field_path=str(field_number),
                    byte_offset=idx,
                )
            val = data[idx : idx + 4]
            idx += 4
            fields.append(ProtobufField(field_number=field_number, wire_type=5, value=val))
        elif wire_type in (3, 4):
            raise ProtobufParseError(
                f"Unsupported protobuf group wire type {wire_type} for field {field_number}",
                field_path=str(field_number),
                byte_offset=tag_offset,
            )
        else:
            raise ProtobufParseError(
                f"Unknown wire type {wire_type} for field {field_number}",
                field_path=str(field_number),
                byte_offset=tag_offset,
            )

    return fields


def get_varint_field(fields: list[ProtobufField], field_number: int) -> int | None:
    """Extract first varint value for a field number.

    Args:
        fields (list[ProtobufField]): Decoded fields list.
        field_number (int): Target field number.

    Returns:
        int | None: Varint integer if found, otherwise None.
    """
    for f in fields:
        if f.field_number == field_number and f.wire_type == 0 and isinstance(f.value, int):
            return f.value
    return None


def get_bytes_field(fields: list[ProtobufField], field_number: int) -> bytes | None:
    """Extract first length-delimited bytes payload for a field number.

    Args:
        fields (list[ProtobufField]): Decoded fields list.
        field_number (int): Target field number.

    Returns:
        bytes | None: Byte payload if found, otherwise None.
    """
    for f in fields:
        if f.field_number == field_number and f.wire_type == 2 and isinstance(f.value, bytes):
            return f.value
    return None


def get_all_bytes_fields(fields: list[ProtobufField], field_number: int) -> list[bytes]:
    """Extract all length-delimited bytes payloads for a repeated field number.

    Args:
        fields (list[ProtobufField]): Decoded fields list.
        field_number (int): Target field number.

    Returns:
        list[bytes]: List of byte payloads in source order.
    """
    result: list[bytes] = []
    for f in fields:
        if f.field_number == field_number and f.wire_type == 2 and isinstance(f.value, bytes):
            result.append(f.value)
    return result


def get_string_field(fields: list[ProtobufField], field_number: int) -> str | None:
    """Extract and UTF-8 decode a string field.

    Args:
        fields (list[ProtobufField]): Decoded fields list.
        field_number (int): Target field number.

    Returns:
        str | None: Decoded string if found, otherwise None.

    Raises:
        ProtobufParseError: If byte content is not valid UTF-8.
    """
    raw_bytes = get_bytes_field(fields, field_number)
    if raw_bytes is None:
        return None
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as err:
        raise ProtobufParseError(
            f"Invalid UTF-8 string in field {field_number}: {err}",
            field_path=str(field_number),
        ) from err
