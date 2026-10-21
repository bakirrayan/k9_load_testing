//! Compressed responses (decoded, counted as sent), compressed request
//! bodies, and multipart forms, against the test server's own encoders and
//! multipart parser.

use k9_http::compression::{Encoding, compress};
use k9_http::multipart::Form;
use k9_http::{Client, ClientConfig, HttpError, Request};
use k9_testserver::TestServer;

fn client() -> Client {
    Client::new(ClientConfig::default()).unwrap()
}

#[tokio::test]
async fn responses_are_decompressed_and_counted_as_they_came() {
    let server = TestServer::start().await.unwrap();
    for encodings in ["gzip", "deflate", "br", "zstd", "gzip, br"] {
        let url = server.url(&format!("/compressed/{encodings}"));
        let res = client().send(&Request::get(&url).unwrap()).await.unwrap();
        assert_eq!(res.status, 200, "{encodings}");
        let json = res.json().unwrap_or_else(|e| panic!("{encodings}: {e}"));
        assert_eq!(json["compressed"], encodings);
        assert_eq!(json["acceptEncoding"], "gzip, deflate", "k6's default");
        assert!(
            res.bytes_received < res.body.len() as u64,
            "{encodings}: {} on the wire for {} bytes",
            res.bytes_received,
            res.body.len()
        );
    }
}

#[tokio::test]
async fn a_request_may_ask_for_other_encodings() {
    let server = TestServer::start().await.unwrap();
    let mut request = Request::get(&server.url("/compressed/zstd")).unwrap();
    request
        .headers
        .insert("accept-encoding", "zstd".parse().unwrap());
    let res = client().send(&request).await.unwrap();
    assert_eq!(res.json().unwrap()["acceptEncoding"], "zstd");
}

#[tokio::test]
async fn a_body_that_isnt_what_it_claims_fails_the_request() {
    let server = TestServer::start().await.unwrap();
    let err = client()
        .send(&Request::get(&server.url("/compressed/broken")).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err, HttpError::Decompress(_)), "{err:?}");
    assert!(err.to_string().contains("gzip"), "{err}");
    // Discarded bodies aren't decoded at all.
    let discarding = Client::new(ClientConfig {
        discard_body: true,
        ..ClientConfig::default()
    })
    .unwrap();
    let res = discarding
        .send(&Request::get(&server.url("/compressed/broken")).unwrap())
        .await
        .unwrap();
    assert!(res.body.is_empty());
}

#[tokio::test]
async fn request_bodies_can_be_compressed() {
    let server = TestServer::start().await.unwrap();
    for list in ["gzip", "deflate", "br", "zstd", "gzip, zstd"] {
        let encodings = Encoding::parse_list(list).unwrap();
        let mut request = Request::new(http::Method::POST, &server.url("/echo")).unwrap();
        request.body = compress(b"hello compressed world", &encodings).unwrap();
        request
            .headers
            .insert("content-encoding", list.parse().unwrap());
        let res = client().send(&request).await.unwrap();
        assert_eq!(res.status, 200, "{list}: {}", res.text());
        assert_eq!(
            res.json().unwrap()["body"],
            "hello compressed world",
            "{list}"
        );
    }
    let err = Encoding::parse_list("gzip, lzma").unwrap_err();
    assert!(err.contains("lzma"), "{err}");
}

#[tokio::test]
async fn multipart_forms_carry_fields_and_files() {
    let server = TestServer::start().await.unwrap();
    let (content_type, body) = Form::new()
        .text("user", "zoë")
        .text("note", "two\r\nlines")
        .file(
            "avatar",
            "me \"1\".png",
            Some("image/png"),
            b"\x89PNG\x00data",
        )
        .file("notes", "notes.txt", None, b"plain")
        .finish();
    assert!(content_type.starts_with("multipart/form-data; boundary="));
    let mut request = Request::new(http::Method::POST, &server.url("/multipart")).unwrap();
    request
        .headers
        .insert("content-type", content_type.parse().unwrap());
    request.body = body;
    let res = client().send(&request).await.unwrap();
    assert_eq!(res.status, 200, "{}", res.text());
    let json = res.json().unwrap();
    assert_eq!(json["fields"]["user"], "zoë");
    assert_eq!(json["fields"]["note"], "two\r\nlines");
    let avatar = &json["files"]["avatar"];
    assert_eq!(avatar["filename"], "me \"1\".png");
    assert_eq!(avatar["contentType"], "image/png");
    assert_eq!(avatar["size"], 9);
    assert_eq!(
        json["files"]["notes"]["contentType"],
        "application/octet-stream"
    );
    assert_eq!(json["files"]["notes"]["content"], "plain");
}
