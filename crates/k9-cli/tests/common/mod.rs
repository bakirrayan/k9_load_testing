//! Reading protobuf messages in tests (no schema: fields by number).

#![allow(dead_code)]

#[derive(Debug)]
pub enum Field<'a> {
    Varint(u64),
    Fixed64(u64),
    Bytes(&'a [u8]),
}

impl Field<'_> {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Field::Bytes(b) => b,
            other => panic!("{other:?}"),
        }
    }
    pub fn string(&self) -> String {
        String::from_utf8(self.bytes().to_vec()).unwrap()
    }
    pub fn double(&self) -> f64 {
        match self {
            Field::Fixed64(bits) => f64::from_bits(*bits),
            other => panic!("{other:?}"),
        }
    }
    pub fn varint(&self) -> u64 {
        match self {
            Field::Varint(v) => *v,
            other => panic!("{other:?}"),
        }
    }
}

pub fn fields(mut buf: &[u8]) -> Vec<(u64, Field<'_>)> {
    fn varint(buf: &mut &[u8]) -> u64 {
        let mut value = 0;
        for shift in (0..64).step_by(7) {
            let byte = buf[0];
            *buf = &buf[1..];
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                break;
            }
        }
        value
    }
    let mut out = Vec::new();
    while !buf.is_empty() {
        let key = varint(&mut buf);
        let field = match key & 7 {
            0 => Field::Varint(varint(&mut buf)),
            1 => {
                let bits = u64::from_le_bytes(buf[..8].try_into().unwrap());
                buf = &buf[8..];
                Field::Fixed64(bits)
            }
            2 => {
                let len = varint(&mut buf) as usize;
                let (bytes, rest) = buf.split_at(len);
                buf = rest;
                Field::Bytes(bytes)
            }
            wire => panic!("wire type {wire}"),
        };
        out.push((key >> 3, field));
    }
    out
}

/// A packed `repeated` varint field's values.
#[allow(dead_code)]
pub fn packed(buf: &[u8]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut value = 0;
    let mut shift = 0;
    for byte in buf {
        value |= u64::from(byte & 0x7f) << shift;
        shift += 7;
        if *byte < 0x80 {
            out.push(value);
            (value, shift) = (0, 0);
        }
    }
    out
}
