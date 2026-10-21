//! `Content-Encoding`s: compressing request bodies, and decoding response
//! bodies (as k6 does, whatever the request asked for).

use std::io::{Read, Write};

use bytes::Bytes;

/// What k9 asks for unless a request says otherwise (k6's default).
pub const DEFAULT_ACCEPT_ENCODING: &str = "gzip, deflate";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Gzip,
    /// zlib-wrapped, as HTTP means it (raw deflate is accepted when decoding).
    Deflate,
    Br,
    Zstd,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Gzip => "gzip",
            Encoding::Deflate => "deflate",
            Encoding::Br => "br",
            Encoding::Zstd => "zstd",
        }
    }

    fn parse(name: &str) -> Option<Encoding> {
        match name.to_ascii_lowercase().as_str() {
            "gzip" | "x-gzip" => Some(Encoding::Gzip),
            "deflate" => Some(Encoding::Deflate),
            "br" => Some(Encoding::Br),
            "zstd" => Some(Encoding::Zstd),
            _ => None,
        }
    }

    /// A comma-separated list, e.g. `gzip, br` (applied in that order).
    pub fn parse_list(list: &str) -> Result<Vec<Encoding>, String> {
        list.split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(|name| {
                Encoding::parse(name).ok_or_else(|| {
                    format!("unknown compression `{name}` (use gzip, deflate, br or zstd)")
                })
            })
            .collect()
    }

    /// The `Content-Encoding` value for `encodings`.
    pub fn header_value(encodings: &[Encoding]) -> String {
        encodings
            .iter()
            .map(|e| e.name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// `data` compressed with each of `encodings`, in order.
pub fn compress(data: &[u8], encodings: &[Encoding]) -> std::io::Result<Bytes> {
    let mut data = data.to_vec();
    for encoding in encodings {
        data = match encoding {
            Encoding::Gzip => {
                let mut e =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(&data)?;
                e.finish()?
            }
            Encoding::Deflate => {
                let mut e =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                e.write_all(&data)?;
                e.finish()?
            }
            Encoding::Br => {
                let mut out = Vec::new();
                {
                    let mut e = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                    e.write_all(&data)?;
                }
                out
            }
            Encoding::Zstd => zstd::stream::encode_all(&data[..], 3)?,
        };
    }
    Ok(Bytes::from(data))
}

/// Undoes `content_encoding` (the response header's value). Encodings k9
/// doesn't know, and `identity`, leave the body as it is.
pub(crate) fn decompress(data: Bytes, content_encoding: &str) -> Result<Bytes, String> {
    let mut data = data;
    for name in content_encoding.split(',').map(str::trim).rev() {
        let Some(encoding) = Encoding::parse(name) else {
            continue;
        };
        let mut out = Vec::new();
        let read = match encoding {
            Encoding::Gzip => flate2::read::MultiGzDecoder::new(&data[..]).read_to_end(&mut out),
            Encoding::Deflate => flate2::read::ZlibDecoder::new(&data[..])
                .read_to_end(&mut out)
                .or_else(|_| {
                    out.clear();
                    flate2::read::DeflateDecoder::new(&data[..]).read_to_end(&mut out)
                }),
            Encoding::Br => brotli::Decompressor::new(&data[..], 4096).read_to_end(&mut out),
            Encoding::Zstd => zstd::stream::read::Decoder::new(&data[..])
                .and_then(|mut d| d.read_to_end(&mut out)),
        };
        read.map_err(|e| format!("decoding a {} body: {e}", encoding.name()))?;
        data = Bytes::from(out);
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_encoding_round_trips_in_order() {
        let all = [
            Encoding::Gzip,
            Encoding::Deflate,
            Encoding::Br,
            Encoding::Zstd,
        ];
        let compressed = compress(b"round trip", &all).unwrap();
        let header = Encoding::header_value(&all);
        assert_eq!(header, "gzip, deflate, br, zstd");
        assert_eq!(decompress(compressed, &header).unwrap(), "round trip");
        assert_eq!(
            decompress(Bytes::from("as is"), "identity, x-custom").unwrap(),
            "as is"
        );
    }

    #[test]
    fn raw_deflate_is_accepted() {
        let mut e = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(b"raw").unwrap();
        let raw = Bytes::from(e.finish().unwrap());
        assert_eq!(decompress(raw, "deflate").unwrap(), "raw");
    }
}
