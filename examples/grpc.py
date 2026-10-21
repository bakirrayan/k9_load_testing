"""Unary gRPC against the test server's `k9.test.Echo` service.

    cargo run -p k9-testserver        # in another terminal (gRPC over h2c on :8080)
    k9 run examples/grpc.py
"""

import k9

options = {
    "vus": 5,
    "duration": "10s",
    "thresholds": {
        "grpc_req_duration": ["p(95)<200"],
        "checks": ["rate>0.99"],
    },
}

# Compiled once, when the script loads; shared by every VU.
protos = k9.grpc.load("crates/k9-testserver/proto/k9/test/echo.proto")
client = k9.grpc.Client(k9.env.get("TARGET", "127.0.0.1:8080"), protos, plaintext=True)


async def default() -> None:
    res = await client.invoke(
        "k9.test.Echo/Say",
        {"text": f"hello from VU {k9.vu.id}", "delay_ms": 20},
        metadata={"x-vu": str(k9.vu.id)},
    )
    k9.check(
        res,
        {
            "status is OK": lambda r: r.status == k9.grpc.StatusOK,
            "echoed": lambda r: r.message is not None and r.message["text"].startswith("hello"),
        },
    )
    # A bidirectional stream: send, read the answer, then half-close.
    async with client.stream("k9.test.Echo/Chat") as stream:
        for word in ["one", "two"]:
            await stream.send({"text": word})
            answer = await stream.recv()
            k9.check(answer, {"streamed": lambda a: a is not None and a["text"] == word})
        stream.close_send()
        async for _ in stream:
            pass
    k9.check(stream, {"stream ended OK": lambda s: s.status == k9.grpc.StatusOK})
    await k9.sleep(0.1)
