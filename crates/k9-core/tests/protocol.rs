//! The manager ↔ worker protocol's handshake. (The messages' wire format is
//! tested in k9-grpc.)

use k9_core::protocol::{AgentGroup, FromWorker, PROTOCOL_VERSION, ProtocolError, check_hello};

#[test]
fn workers_of_another_protocol_version_are_refused() {
    let hello = FromWorker::Hello {
        protocol: PROTOCOL_VERSION + 1,
        version: "9.9.9".into(),
        pid: 7,
        slot: 0,
        token: "t".into(),
        group: AgentGroup::default(),
    };
    let err = check_hello(&hello, "t").unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "worker (k9 9.9.9, pid 7) speaks protocol v{}, this k9 speaks v{PROTOCOL_VERSION}: install the same k9 version everywhere",
            PROTOCOL_VERSION + 1
        )
    );
    let ok = FromWorker::Hello {
        protocol: PROTOCOL_VERSION,
        version: "0.0.1".into(),
        pid: 7,
        slot: 0,
        token: "t".into(),
        group: AgentGroup::default(),
    };
    assert_eq!(check_hello(&ok, "t").unwrap(), 7);
    assert!(matches!(
        check_hello(&ok, "other"),
        Err(ProtocolError::Token)
    ));
    assert!(
        check_hello(&FromWorker::Ready, "t").is_err(),
        "Hello must come first"
    );
}
