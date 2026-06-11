//! JSON <-> Parquet encode/decode engine.
//!
//! Exposes two `pyfunc`s:
//!   - `encode(data, encodings=None, compression_level=3) -> bytes`
//!         newline-delimited JSON (one object per line) -> Parquet bytes
//!   - `decode(data) -> str`
//!         Parquet bytes -> newline-delimited JSON
//!
//! Per-column Parquet encoding is either inferred (float columns ->
//! BYTE_STREAM_SPLIT, everything else -> dictionary) or set explicitly via the
//! `encodings` map (`{column: "bss" | "dictionary" | "plain"}`).

use std::collections::HashMap;
use std::io::{BufReader, Cursor, Seek};
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field};
use arrow::json::reader::{ReaderBuilder, infer_json_schema_from_seekable};
use arrow::json::writer::LineDelimitedWriter;

use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

enum ColumnEncoding {
    Dictionary,
    ByteStreamSplit,
    Plain,
}

/// Resolve a column's Parquet encoding: explicit override first, else inferred
/// (float -> BYTE_STREAM_SPLIT, otherwise dictionary).
fn resolve_encoding(
    field: &Field,
    encodings: Option<&HashMap<String, String>>,
) -> Result<ColumnEncoding, String> {
    if let Some(spec) = encodings.and_then(|m| m.get(field.name())) {
        return match spec.to_ascii_lowercase().as_str() {
            "bss" | "byte_stream_split" => Ok(ColumnEncoding::ByteStreamSplit),
            "dict" | "dictionary" => Ok(ColumnEncoding::Dictionary),
            "plain" => Ok(ColumnEncoding::Plain),
            other => Err(format!(
                "unknown encoding {other:?} for column {:?} (expected \
                 'bss'/'byte_stream_split', 'dictionary', or 'plain')",
                field.name()
            )),
        };
    }
    Ok(match field.data_type() {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            ColumnEncoding::ByteStreamSplit
        }
        _ => ColumnEncoding::Dictionary,
    })
}

fn encode_impl(
    data: &[u8],
    encodings: Option<&HashMap<String, String>>,
    compression_level: i32,
) -> Result<Vec<u8>, String> {
    let mut reader = BufReader::new(Cursor::new(data));
    let (schema, _) = infer_json_schema_from_seekable(&mut reader, None)
        .map_err(|e| format!("schema inference failed: {e}"))?;
    reader
        .rewind()
        .map_err(|e| format!("rewind failed: {e}"))?;

    let schema = Arc::new(schema);
    let json_reader = ReaderBuilder::new(schema.clone())
        .build(reader)
        .map_err(|e| format!("json reader build failed: {e}"))?;
    let batches: Vec<RecordBatch> = json_reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("json read failed: {e}"))?;

    let zstd = ZstdLevel::try_new(compression_level)
        .map_err(|e| format!("invalid zstd level {compression_level}: {e}"))?;
    let mut builder = WriterProperties::builder().set_compression(Compression::ZSTD(zstd));
    for field in schema.fields() {
        let path = ColumnPath::new(vec![field.name().clone()]);
        match resolve_encoding(field, encodings)? {
            ColumnEncoding::Dictionary => {
                builder = builder.set_column_dictionary_enabled(path, true);
            }
            ColumnEncoding::ByteStreamSplit => {
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path, Encoding::BYTE_STREAM_SPLIT);
            }
            ColumnEncoding::Plain => {
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path, Encoding::PLAIN);
            }
        }
    }
    let props = builder.build();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
            .map_err(|e| format!("parquet writer init failed: {e}"))?;
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| format!("parquet write failed: {e}"))?;
        }
        writer
            .close()
            .map_err(|e| format!("parquet close failed: {e}"))?;
    }
    Ok(buf)
}

fn decode_impl(data: &[u8]) -> Result<String, String> {
    let bytes = bytes::Bytes::copy_from_slice(data);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| format!("parquet reader init failed: {e}"))?
        .build()
        .map_err(|e| format!("parquet reader build failed: {e}"))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("parquet read failed: {e}"))?;

    let mut out: Vec<u8> = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut out);
        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| format!("json write failed: {e}"))?;
        }
        writer
            .finish()
            .map_err(|e| format!("json finish failed: {e}"))?;
    }
    String::from_utf8(out).map_err(|e| format!("utf-8 decode failed: {e}"))
}

/// Encode newline-delimited JSON into Parquet bytes.
#[pyfunction]
#[pyo3(signature = (data, encodings=None, compression_level=3))]
fn encode<'py>(
    py: Python<'py>,
    data: &[u8],
    encodings: Option<HashMap<String, String>>,
    compression_level: i32,
) -> PyResult<Bound<'py, PyBytes>> {
    let parquet = encode_impl(data, encodings.as_ref(), compression_level)
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &parquet))
}

/// Decode Parquet bytes into newline-delimited JSON.
#[pyfunction]
fn decode(data: &[u8]) -> PyResult<String> {
    decode_impl(data).map_err(PyValueError::new_err)
}

#[pymodule]
fn parquet_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    Ok(())
}
