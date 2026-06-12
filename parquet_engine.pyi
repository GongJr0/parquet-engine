"""JSON <-> Parquet encode/decode engine (Arrow + Parquet, in Rust)."""

from collections.abc import Mapping

def encode(
    data: bytes,
    encodings: Mapping[str, str] | None = ...,
    compression_level: int = ...,
) -> bytes:
    """Encode newline-delimited JSON (one object per line) into Parquet bytes.

    Args:
        data: NDJSON payload (UTF-8 bytes), one JSON object per line.
        encodings: Optional per-column Parquet encoding overrides, mapping a
            column name to ``"bss"``/``"byte_stream_split"``, ``"dict"``/
            ``"dictionary"``, or ``"plain"``. Columns left unspecified are
            inferred (float columns -> BYTE_STREAM_SPLIT, others -> dictionary).
        compression_level: ZSTD compression level (default ``3``).

    Returns:
        The encoded Parquet file as bytes.

    Raises:
        ValueError: If schema inference, encoding resolution, or Parquet
            writing fails.
    """
    ...

def decode(data: bytes) -> str:
    """Decode Parquet bytes into newline-delimited JSON.

    Args:
        data: A Parquet file as bytes.

    Returns:
        Newline-delimited JSON (one object per line) as a UTF-8 string.

    Raises:
        ValueError: If the Parquet input cannot be read or decoded.
    """
    ...
