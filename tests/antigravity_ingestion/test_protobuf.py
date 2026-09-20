"""Tests for low-level Protobuf wire-format reader."""

from __future__ import annotations

import pytest

from coding_agent_usage_monitors.antigravity_token_usage.ingestion.errors import ProtobufParseError
from coding_agent_usage_monitors.antigravity_token_usage.ingestion.protobuf import (
    read_varint,
    ProtobufField,
    parse_protobuf,
    get_bytes_field,
    get_string_field,
    get_all_bytes_fields,
)


def encode_varint(val: int) -> bytes:
    """Encode an integer into LEB128 varint bytes."""
    res = bytearray()
    while True:
        b = val & 0x7F
        val >>= 7
        if val != 0:
            res.append(b | 0x80)
        else:
            res.append(b)
            break
    return bytes(res)


def test_read_varint_single_and_multi_byte() -> None:
    """Verify single and multi-byte LEB128 varint decoding."""
    val, offset = read_varint(encode_varint(0), 0)
    assert val == 0 and offset == 1

    val, offset = read_varint(encode_varint(150), 0)
    assert val == 150 and offset == 2

    val, offset = read_varint(encode_varint(300000), 0)
    assert val == 300000


def test_parse_fixed32_fixed64_and_bytes() -> None:
    """Verify decoding of fixed32, fixed64, and bytes/string wire types."""
    # Wire type 1 (Fixed64), field 1
    tag_fixed64 = encode_varint((1 << 3) | 1)
    payload_fixed64 = b"\x01\x02\x03\x04\x05\x06\x07\x08"

    # Wire type 5 (Fixed32), field 2
    tag_fixed32 = encode_varint((2 << 3) | 5)
    payload_fixed32 = b"\x0a\x0b\x0c\x0d"

    # Wire type 2 (Bytes), field 3
    tag_bytes = encode_varint((3 << 3) | 2)
    bytes_data = b"hello world"
    payload_bytes = encode_varint(len(bytes_data)) + bytes_data

    blob = tag_fixed64 + payload_fixed64 + tag_fixed32 + payload_fixed32 + tag_bytes + payload_bytes
    fields = parse_protobuf(blob)

    assert len(fields) == 3
    assert fields[0] == ProtobufField(field_number=1, wire_type=1, value=payload_fixed64)
    assert fields[1] == ProtobufField(field_number=2, wire_type=5, value=payload_fixed32)
    assert fields[2] == ProtobufField(field_number=3, wire_type=2, value=bytes_data)

    assert get_bytes_field(fields, 3) == bytes_data
    assert get_string_field(fields, 3) == "hello world"


def test_preserve_repeated_fields_in_order() -> None:
    """Verify repeated length-delimited fields are preserved in source order."""
    tag = encode_varint((10 << 3) | 2)
    blob = tag + encode_varint(3) + b"AAA" + tag + encode_varint(3) + b"BBB" + tag + encode_varint(3) + b"CCC"
    fields = parse_protobuf(blob)

    repeated = get_all_bytes_fields(fields, 10)
    assert repeated == [b"AAA", b"BBB", b"CCC"]


def test_reject_truncated_tags_values_and_payloads() -> None:
    """Verify rejection of truncated varints, fixed-width values, and byte payloads."""
    # Truncated varint byte stream with MSB set
    with pytest.raises(ProtobufParseError, match="Truncated varint"):
        parse_protobuf(b"\x80\x80")

    # Truncated fixed64
    tag_fixed64 = encode_varint((1 << 3) | 1)
    with pytest.raises(ProtobufParseError, match="Truncated fixed64"):
        parse_protobuf(tag_fixed64 + b"\x01\x02")

    # Truncated fixed32
    tag_fixed32 = encode_varint((2 << 3) | 5)
    with pytest.raises(ProtobufParseError, match="Truncated fixed32"):
        parse_protobuf(tag_fixed32 + b"\x01")

    # Length prefix extending beyond blob
    tag_bytes = encode_varint((3 << 3) | 2)
    with pytest.raises(ProtobufParseError, match="extends beyond blob boundary"):
        parse_protobuf(tag_bytes + encode_varint(100) + b"short")


def test_reject_overflowed_or_unterminated_varints() -> None:
    """Verify varints exceeding the protobuf uint64 range raise ProtobufParseError."""
    overflow_varint = b"\x80" * 11
    with pytest.raises(ProtobufParseError, match="Varint overflow"):
        read_varint(overflow_varint, 0)

    tenth_byte_overflow = (b"\x80" * 9) + b"\x02"
    with pytest.raises(ProtobufParseError, match="Varint overflow"):
        read_varint(tenth_byte_overflow, 0)


def test_reject_group_wire_types_and_invalid_field_zero() -> None:
    """Verify rejection of group start/end wire types and field_number=0."""
    # Wire type 3 (Group start)
    tag_group_start = encode_varint((1 << 3) | 3)
    with pytest.raises(ProtobufParseError, match="Unsupported protobuf group wire type"):
        parse_protobuf(tag_group_start)

    # Wire type 4 (Group end)
    tag_group_end = encode_varint((1 << 3) | 4)
    with pytest.raises(ProtobufParseError, match="Unsupported protobuf group wire type"):
        parse_protobuf(tag_group_end)

    # Field number 0
    tag_zero = encode_varint((0 << 3) | 0)
    with pytest.raises(ProtobufParseError, match="Invalid field number 0"):
        parse_protobuf(tag_zero)


def test_reject_invalid_utf8_in_string_fields() -> None:
    """Verify contextual error when decoding non-UTF8 bytes as string."""
    tag = encode_varint((5 << 3) | 2)
    invalid_utf8 = b"\xff\xfe\xfd"
    blob = tag + encode_varint(len(invalid_utf8)) + invalid_utf8
    fields = parse_protobuf(blob)

    with pytest.raises(ProtobufParseError, match="Invalid UTF-8 string"):
        get_string_field(fields, 5)
