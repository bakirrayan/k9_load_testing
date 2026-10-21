# Scaling out

## Across cores: `--workers`

`k9 run --workers N` (or `auto`, one per core) runs the test in N processes on
one machine, so a Python script gets N interpreters. Hooks, thresholds, outputs
and the summary stay in the manager, and totals stay exact. See
[Python scripts](scripts.md) for the details.

## Across machines: `--agents`

`k9 run --agents N` runs the test on N `k9 agent`s, typically one per load
machine, instead of locally. The manager (the machine you run `k9 run` on) runs
the hooks, thresholds, outputs and summary, as with `--workers`. The agents run
the VUs, one execution segment each.

```sh
# On each load machine (they wait for tests and serve one after another):
k9 agent --manager manager.example:6570 --token "$K9_AGENT_TOKEN"
# On the manager:
k9 run script.py --agents 3 --agent-token "$K9_AGENT_TOKEN"
```

- **Connecting:** the manager listens on `--listen` (default `0.0.0.0:6570`) and
  waits up to 10 minutes for the agents. Agents started first wait for it.
- **The token:** agents must present `--agent-token` (or `K9_AGENT_TOKEN`). A wrong
  one is refused before anything is sent. Without a token, the manager makes one
  up and prints the `k9 agent` command to run.
- **Workers and weights:** `k9 agent --workers N` (or `auto`: one per core) runs
  the agent's part in N processes, for Python scripts that need more than one
  interpreter's worth of CPU; `--agents` still counts agents. Agents share the
  test equally unless `k9 agent --weight W` (or `auto`: its core count) says
  otherwise, e.g. an agent with `--weight 2` gets twice the VUs, iterations and
  rate of one with the default 1. The manager prints each agent's share.
- **What agents receive:** they can't read the manager's files, so the manager
  sends the script with the local modules it imports (the `.py` files under its
  directory, found by reading `import` statements), and puts the plan's files
  into the plan: feeder rows, `tlsAuth` PEMs, compiled `.proto` files and
  multipart uploads. Files a script opens itself, and modules imported by
  computed names, must be installed on the agents too.
- **TLS:** `--tls-cert`/`--tls-key` (PEM) make the manager use TLS. Agents then
  connect to `https://host:6570`, trusting the manager's certificate through
  `--tls-ca`, or the public web roots by default.
- **Failures:** an agent that disconnects mid-run fails the run (exit code 103),
  as a crashed worker does. The summary still prints.
- **Versions:** agents need the same k9 version as the manager (the protocol is
  checked on connect). Python scripts need the wheel on the agents, i.e.
  `python -m k9 agent`.

The protocol is gRPC, typed in
[`worker.proto`](../crates/k9-grpc/proto/k9/worker/v1/worker.proto) so
other tools can drive or watch k9 workers.

## By hand: execution segments

To split one test across machines by hand, give each instance a part with k6's
execution-segment flags; the parts add up to the whole test (exact iteration
totals, distinct VU ids), and the header shows each part's share:

```console
$ k9 run plan.yaml --execution-segment 0:1/3   --execution-segment-sequence 0,1/3,2/3,1
$ k9 run plan.yaml --execution-segment 1/3:2/3 --execution-segment-sequence 0,1/3,2/3,1
$ k9 run plan.yaml --execution-segment 2/3:1   --execution-segment-sequence 0,1/3,2/3,1
```

Each instance prints its own summary (and runs a script's `setup()`/`teardown()`
itself). On one machine, `--workers N` does all of this for you (see below).
