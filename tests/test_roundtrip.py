from __future__ import annotations

import json

import pytest

import parquet_engine as pe


def _ndjson(rows: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode()


def _rows(n: int = 500) -> list[dict]:
    import random

    rng = random.Random(0)
    return [
        {"x": rng.gauss(0, 1), "y": rng.gauss(5, 2), "g": i % 4, "label": "ab"[i % 2]}
        for i in range(n)
    ]


def test_round_trip_is_exact() -> None:
    rows = _rows()
    parquet = pe.encode(_ndjson(rows))
    assert isinstance(parquet, bytes) and len(parquet) > 0

    out = [json.loads(line) for line in pe.decode(parquet).splitlines() if line.strip()]
    assert out == rows


def test_default_inference_uses_bss_for_floats() -> None:
    ndjson = _ndjson(_rows())
    # Default inference should match an explicit BSS request on the float columns
    # and beat dictionary encoding on full-precision floats.
    default = len(pe.encode(ndjson))
    bss = len(pe.encode(ndjson, {"x": "bss", "y": "bss"}))
    dictionary = len(pe.encode(ndjson, {"x": "dictionary", "y": "dictionary"}))
    assert default == bss
    assert bss < dictionary


def test_explicit_encodings_and_compression_level() -> None:
    ndjson = _ndjson(_rows())
    for spec in ("bss", "byte_stream_split", "dictionary", "dict", "plain"):
        assert len(pe.encode(ndjson, {"x": spec})) > 0
    assert len(pe.encode(ndjson, None, 19)) > 0


def test_unknown_encoding_raises() -> None:
    with pytest.raises(ValueError):
        pe.encode(_ndjson(_rows()), {"x": "bogus"})
