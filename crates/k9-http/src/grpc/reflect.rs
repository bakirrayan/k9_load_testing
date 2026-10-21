//! Server reflection (`grpc.reflection.v1`, else `v1alpha`): the services a
//! server offers, and the `.proto` descriptors they're defined in. One call
//! per round: every question is sent, then the answers are read.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use bytes::BytesMut;
use http::HeaderMap;
use prost::Message;
use prost_reflect::DescriptorPool;
use prost_types::FileDescriptorProto;
use url::Url;

use super::{GrpcError, OK, Protos, frame, frames, status_name, status_of};
use crate::Client;

const SERVICES: [&str; 2] = [
    "grpc.reflection.v1.ServerReflection",
    "grpc.reflection.v1alpha.ServerReflection",
];

/// Rounds of asking for missing imports before giving up.
const MAX_ROUNDS: usize = 16;

#[derive(Clone, PartialEq, Message)]
struct ReflectionRequest {
    #[prost(string, tag = "1")]
    host: String,
    #[prost(oneof = "Ask", tags = "3, 4, 7")]
    ask: Option<Ask>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum Ask {
    #[prost(string, tag = "3")]
    FileByFilename(String),
    #[prost(string, tag = "4")]
    FileContainingSymbol(String),
    #[prost(string, tag = "7")]
    ListServices(String),
}

#[derive(Clone, PartialEq, Message)]
struct ReflectionResponse {
    #[prost(oneof = "Answer", tags = "4, 6, 7")]
    answer: Option<Answer>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum Answer {
    #[prost(message, tag = "4")]
    Files(FileDescriptorResponse),
    #[prost(message, tag = "6")]
    Services(ListServiceResponse),
    #[prost(message, tag = "7")]
    Error(ErrorResponse),
}

#[derive(Clone, PartialEq, Message)]
struct FileDescriptorResponse {
    #[prost(bytes = "vec", repeated, tag = "1")]
    file_descriptor_proto: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct ListServiceResponse {
    #[prost(message, repeated, tag = "1")]
    service: Vec<ServiceResponse>,
}

#[derive(Clone, PartialEq, Message)]
struct ServiceResponse {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct ErrorResponse {
    #[prost(int32, tag = "1")]
    error_code: i32,
    #[prost(string, tag = "2")]
    error_message: String,
}

/// Why a round failed.
enum Failure {
    /// The server has no such reflection service.
    Unimplemented(String),
    Other(String),
}

impl Protos {
    /// Asks the server at `target` (over `client`, see [`Client::grpc`]) for
    /// its services and their descriptors. `timeout` applies to each call.
    pub async fn reflect(
        client: &Client,
        target: &Url,
        timeout: Option<Duration>,
    ) -> Result<Protos, GrpcError> {
        let mut unimplemented = String::new();
        for service in SERVICES {
            let url = target
                .join(&format!("/{service}/ServerReflectionInfo"))
                .expect("a path joins any base URL");
            match reflect_with(client, &url, timeout).await {
                Ok(protos) => return Ok(protos),
                Err(Failure::Unimplemented(message)) => unimplemented = message,
                Err(Failure::Other(message)) => {
                    return Err(GrpcError::Reflection(format!("{target}: {message}")));
                }
            }
        }
        Err(GrpcError::Reflection(format!(
            "{target} has no reflection service (UNIMPLEMENTED: {unimplemented})"
        )))
    }
}

async fn reflect_with(
    client: &Client,
    url: &Url,
    timeout: Option<Duration>,
) -> Result<Protos, Failure> {
    let mut services = Vec::new();
    for answer in ask(client, url, vec![Ask::ListServices(String::new())], timeout).await? {
        if let Answer::Services(list) = answer {
            services.extend(list.service.into_iter().map(|s| s.name));
        }
    }
    let mut asks: Vec<Ask> = services
        .into_iter()
        .filter(|name| !name.starts_with("grpc.reflection."))
        .map(Ask::FileContainingSymbol)
        .collect();
    let mut files = BTreeMap::new();
    for _ in 0..MAX_ROUNDS {
        if asks.is_empty() {
            break;
        }
        for answer in ask(client, url, asks, timeout).await? {
            let Answer::Files(found) = answer else {
                return Err(Failure::Other("unexpected answer".to_owned()));
            };
            for bytes in found.file_descriptor_proto {
                let file = FileDescriptorProto::decode(bytes.as_slice())
                    .map_err(|e| Failure::Other(format!("invalid descriptor: {e}")))?;
                files.entry(file.name().to_owned()).or_insert(file);
            }
        }
        let missing: HashSet<&String> = files
            .values()
            .flat_map(|f| &f.dependency)
            .filter(|dep| !files.contains_key(*dep))
            .collect();
        asks = missing
            .into_iter()
            .map(|dep| Ask::FileByFilename(dep.clone()))
            .collect();
    }
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_protos(in_dependency_order(&files))
        .map_err(|e| Failure::Other(e.to_string()))?;
    Ok(Protos { pool })
}

/// One round: sends every question, half-closes, and reads every answer.
async fn ask(
    client: &Client,
    url: &Url,
    asks: Vec<Ask>,
    timeout: Option<Duration>,
) -> Result<Vec<Answer>, Failure> {
    let mut body = BytesMut::new();
    for ask in asks {
        let request = ReflectionRequest {
            host: String::new(),
            ask: Some(ask),
        };
        frame(&request, &mut body);
    }
    let res = client
        .grpc_send(url, &HeaderMap::new(), body.freeze(), timeout, false)
        .await
        .map_err(|e| Failure::Other(e.to_string()))?;
    match status_of(&res) {
        (OK, _) => {}
        (12, message) => return Err(Failure::Unimplemented(message)),
        (code, message) => {
            return Err(Failure::Other(format!("{} {message}", status_name(code))));
        }
    }
    let mut answers = Vec::new();
    let encoding = res
        .headers
        .get("grpc-encoding")
        .and_then(|v| v.to_str().ok());
    let payloads =
        frames(&res.body, encoding, None).map_err(|(_, reason)| Failure::Other(reason))?;
    for payload in payloads {
        let response = ReflectionResponse::decode(payload)
            .map_err(|e| Failure::Other(format!("invalid answer: {e}")))?;
        match response.answer {
            Some(Answer::Error(e)) => {
                return Err(Failure::Other(format!(
                    "{} ({})",
                    e.error_message,
                    status_name(e.error_code as u32)
                )));
            }
            Some(answer) => answers.push(answer),
            None => return Err(Failure::Other("empty answer".to_owned())),
        }
    }
    Ok(answers)
}

/// Each file after the files it imports.
fn in_dependency_order(files: &BTreeMap<String, FileDescriptorProto>) -> Vec<FileDescriptorProto> {
    fn visit<'a>(
        name: &'a str,
        files: &'a BTreeMap<String, FileDescriptorProto>,
        seen: &mut HashSet<&'a str>,
        order: &mut Vec<FileDescriptorProto>,
    ) {
        if !seen.insert(name) {
            return;
        }
        if let Some(file) = files.get(name) {
            for dep in &file.dependency {
                visit(dep, files, seen, order);
            }
            order.push(file.clone());
        }
    }
    let mut seen = HashSet::new();
    let mut order = Vec::new();
    for name in files.keys() {
        visit(name, files, &mut seen, &mut order);
    }
    order
}
