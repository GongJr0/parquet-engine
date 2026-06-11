# parquet-engine

A small, fast JSON ⇄ Parquet encode/decode engine written in Rust (Arrow +
Parquet via PyO3), distributed as a portable `abi3` wheel — no PyArrow / pandas
dependency.

It exists so projects can read and write Parquet (with column-aware encoding for
float-heavy data) without pulling the large PyArrow wheel.

## Install

```bash
pip install parquet-engine
```

The published wheels are `abi3` (built once per platform, work on CPython ≥ 3.10).

## Usage

The engine speaks **newline-delimited JSON** (one JSON object per line):

```python
import json
import parquet_engine as pe

rows = [{"x": 0.1, "y": 1.5, "g": 0}, {"x": 0.2, "y": 1.6, "g": 1}]
ndjson = ("\n".join(json.dumps(r) for r in rows) + "\n").encode()

parquet_bytes = pe.encode(ndjson)            # NDJSON  -> Parquet (bytes)
ndjson_back   = pe.decode(parquet_bytes)     # Parquet -> NDJSON (str)
```

### API

```python
encode(data: bytes, encodings: dict[str, str] | None = None,
       compression_level: int = 3) -> bytes
decode(data: bytes) -> str
```

- **`encode`** infers a schema from the NDJSON, writes Parquet with ZSTD
  compression (`compression_level`, default 3), and chooses a per-column
  encoding.
- **`decode`** reads Parquet bytes back into newline-delimited JSON.

### Column encoding

By default the encoding is **inferred** per column:

| Column type | Encoding |
|-------------|----------|
| floating point (`f16`/`f32`/`f64`) | `BYTE_STREAM_SPLIT` (dictionary off) |
| everything else | dictionary |

This matches the empirical result that byte-stream-split beats dictionary on
full-precision floats, while dictionary wins on low-cardinality columns.

Override per column with the `encodings` map:

```python
pe.encode(ndjson, {"x": "bss", "y": "byte_stream_split",
                   "g": "dictionary", "id": "plain"})
```

Accepted values: `"bss"` / `"byte_stream_split"`, `"dictionary"` / `"dict"`,
`"plain"`. Unknown values raise `ValueError`. Columns absent from the map fall
back to inference.

## Building from source

```bash
maturin develop          # build + install into the active venv
maturin build --release  # produce a wheel
```

> **Windows + MSVC note:** if your shell sets `CC`/`CXX` to MinGW (e.g. msys2),
> the bundled `zstd` C build will mismatch the MSVC linker. Build with those
> unset so the `cc` crate auto-detects `cl.exe`:
> `env -u CC -u CXX maturin develop`.

## License

MIT
