//! `k9.test.Echo` (`proto/k9/test/echo.proto`), served by tonic: an
//! independent gRPC implementation for k9's own client to talk to.

use std::pin::Pin;
use std::time::Duration;

use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

mod pb {
    tonic::include_proto!("k9.test");
}

pub use pb::echo_server::EchoServer;

/// `echo.proto` compiled: a `FileDescriptorSet`, for reflection services.
pub const DESCRIPTORS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/echo.bin"));

/// Server reflection (v1 and v1alpha) for `k9.test.Echo`.
pub fn reflection() -> (
    tonic_reflection::server::v1::ServerReflectionServer<
        impl tonic_reflection::server::v1::ServerReflection,
    >,
    tonic_reflection::server::v1alpha::ServerReflectionServer<
        impl tonic_reflection::server::v1alpha::ServerReflection,
    >,
) {
    let builder = || {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(DESCRIPTORS)
    };
    (
        builder().build_v1().expect("valid descriptors"),
        builder().build_v1alpha().expect("valid descriptors"),
    )
}
use pb::{Kind, SayReply, SayRequest};

#[derive(Debug, Default)]
pub struct Echo;

fn answer(req: &SayRequest, metadata: &tonic::metadata::MetadataMap) -> Result<SayReply, Status> {
    if req.status != 0 {
        return Err(Status::new(tonic::Code::from(req.status as i32), &req.text));
    }
    let metadata = metadata
        .iter()
        .filter_map(|kv| match kv {
            tonic::metadata::KeyAndValueRef::Ascii(k, v) if k.as_str().starts_with("x-") => {
                Some((k.to_string(), v.to_str().unwrap_or_default().to_owned()))
            }
            _ => None,
        })
        .collect();
    Ok(SayReply {
        text: req.text.clone(),
        metadata,
        length: req.text.len() as i64,
        empty: false,
        kind: if req.text.len() > 10 {
            Kind::Long
        } else {
            Kind::Short
        } as i32,
        words: req.text.split_whitespace().map(str::to_owned).collect(),
    })
}

type ReplyStream = Pin<Box<dyn Stream<Item = Result<SayReply, Status>> + Send>>;

#[tonic::async_trait]
impl pb::echo_server::Echo for Echo {
    async fn say(&self, request: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
        let req = request.get_ref();
        tokio::time::sleep(Duration::from_millis(req.delay_ms.into())).await;
        let mut response = Response::new(answer(req, request.metadata())?);
        response
            .metadata_mut()
            .insert("x-served-by", MetadataValue::from_static("k9-testserver"));
        Ok(response)
    }

    type ChatStream = ReplyStream;

    async fn chat(
        &self,
        request: Request<Streaming<SayRequest>>,
    ) -> Result<Response<ReplyStream>, Status> {
        let metadata = request.metadata().clone();
        let replies = request.into_inner().then(move |req| {
            let metadata = metadata.clone();
            async move {
                let req = req?;
                tokio::time::sleep(Duration::from_millis(req.delay_ms.into())).await;
                answer(&req, &metadata)
            }
        });
        Ok(Response::new(Box::pin(replies)))
    }
}
