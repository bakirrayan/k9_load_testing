//! `multipart/form-data` bodies: text fields and files, quoted the way Go's
//! `mime/multipart` (and so k6) does.

use bytes::{BufMut, Bytes, BytesMut};

pub struct Form {
    boundary: String,
    body: BytesMut,
}

impl Default for Form {
    fn default() -> Self {
        Form::new()
    }
}

impl Form {
    pub fn new() -> Form {
        let random: String = std::iter::repeat_with(fastrand::alphanumeric)
            .take(24)
            .collect();
        Form {
            boundary: format!("k9-{random}"),
            body: BytesMut::new(),
        }
    }

    pub fn text(mut self, name: &str, value: &str) -> Form {
        self.part(name, None, None, value.as_bytes());
        self
    }

    /// A file part; `content_type` defaults to `application/octet-stream`.
    pub fn file(
        mut self,
        name: &str,
        filename: &str,
        content_type: Option<&str>,
        data: impl AsRef<[u8]>,
    ) -> Form {
        self.part(name, Some(filename), content_type, data.as_ref());
        self
    }

    /// The `Content-Type` header value (with the boundary) and the body.
    pub fn finish(mut self) -> (String, Bytes) {
        self.body.put_slice(b"--");
        self.body.put_slice(self.boundary.as_bytes());
        self.body.put_slice(b"--\r\n");
        (
            format!("multipart/form-data; boundary={}", self.boundary),
            self.body.freeze(),
        )
    }

    fn part(
        &mut self,
        name: &str,
        filename: Option<&str>,
        content_type: Option<&str>,
        data: &[u8],
    ) {
        let b = &mut self.body;
        b.put_slice(b"--");
        b.put_slice(self.boundary.as_bytes());
        b.put_slice(b"\r\nContent-Disposition: form-data; name=\"");
        b.put_slice(quote(name).as_bytes());
        b.put_u8(b'"');
        if let Some(filename) = filename {
            b.put_slice(b"; filename=\"");
            b.put_slice(quote(filename).as_bytes());
            b.put_u8(b'"');
            b.put_slice(b"\r\nContent-Type: ");
            b.put_slice(
                content_type
                    .unwrap_or("application/octet-stream")
                    .as_bytes(),
            );
        } else if let Some(content_type) = content_type {
            b.put_slice(b"\r\nContent-Type: ");
            b.put_slice(content_type.as_bytes());
        }
        b.put_slice(b"\r\n\r\n");
        b.put_slice(data);
        b.put_slice(b"\r\n");
    }
}

/// Backslash-escapes `\` and `"`; line breaks can't appear in a header.
fn quote(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\r', "%0D")
        .replace('\n', "%0A")
}
