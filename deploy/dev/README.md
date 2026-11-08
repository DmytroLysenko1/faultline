# Step 1 demo: OTel Demo → faultline → service graph

`make demo-1` runs the OpenTelemetry Demo in a local kind cluster, sends its traces to faultline and checks the graph faultline builds against the reference edges in `test/demo/otel-demo/`.

## Before you start

- Docker with at least **8 GB** of VM memory (Docker Desktop: Settings → Resources; OTel Demo alone needs about 7 GiB, faultline adds up to 2 GiB).
- `kind`, `kubectl`, `helm` and `go`. The pinned versions are in `hack/versions.env`; `hack/check-versions.sh` warns about differences but does not stop the demo.
- Local port 18080 free, or set `DEMO_PORT` to another port.

## Run

```sh
make demo-1
```

The script works in the kind cluster `faultline-demo` and always passes `--context kind-faultline-demo`, so your current kubectl context is never touched. It is safe to run again on top of a running demo: the cluster is reused, faultline is rebuilt and restarted, OTel Demo is upgraded in place.

Steps, each logged as `demo-1: …`:

1. Build `tmp/bin/faultctl` and `tmp/bin/democheck`.
2. Create or reuse the single-node cluster from `deploy/kind/kind-config-demo.yaml`. A cluster built from another node image is recreated.
3. Build `faultline:dev`, load it into kind, apply `deploy/dev/skeleton`, wait for the rollout.
4. Install OTel Demo (chart version from `versions.env`) with `deploy/dev/otel-demo-values.yaml` and `deploy/dev/otel-demo-faultline-values.yaml`, wait until every Deployment and the collector DaemonSet are ready. The first run pulls about 25 images and takes 10–15 minutes.
5. Port-forward the faultline API to `127.0.0.1:$DEMO_PORT`, poll the graph every 10 s for up to 5 minutes until every required edge is there.
6. Wait for a `pipeline.stats` log record, then run the full check.

A passing run ends with:

```
15/15 required, 0 optional, 15 edges, I1 ok, I3 skipped
demo-1: demo passed; graph in tmp/demo/graph.json
```

The optional count changes from run to run. I3 (the sum of edge calls equals `matched − graph_overflow`) is checked only when the last `pipeline.stats` record has the same revision as the exported graph; under live traffic the graph usually moves on between the two, so `skipped` is normal.

Expect a large share of `expired` halves in `pipeline.stats`: the load generator calls `frontend-proxy` without client spans, and Envoy's client spans to some upstreams get no server span back. These are visible as `unanswered_calls` on the caller node and never become edges.

## Look at the graph

```sh
kubectl --context kind-faultline-demo -n faultline port-forward svc/faultline 18080:8080 &
tmp/bin/faultctl graph export -o dot --server http://127.0.0.1:18080 | dot -Tsvg > graph.svg
tmp/bin/faultctl graph export -o json --server http://127.0.0.1:18080 | jq '.edges | length'
```

`faultctl` waits 10 s by default; over a slow port-forward use `--timeout 1m`.

With `grpcurl` (reflection is on), large graphs need a bigger message limit than the default 4 MiB:

```sh
grpcurl -plaintext -max-msg-sz 83886080 127.0.0.1:18080 faultline.v1.GraphService/GetGraph
```

## When the check fails

`make demo-check` re-runs the check on the last saved graph (`tmp/demo/graph.json`) and log (`tmp/demo/faultline.log`).

| Output | What it means | What to look at |
|---|---|---|
| `missing required edge: A -> B` | A call that happens in every run never paired | the collector reached faultline? `kubectl … -n otel-demo logs daemonset/otel-collector-agent`; `pipeline.stats` counters `expired`, `unpairable` |
| `phantom edge: A -> B` | faultline shows a call that is in neither reference list | a pairing bug, or a new edge in a newer demo version: never add it to `optional-edges.txt` without a reason |
| `node without namespace: svc:/x` | a service lost its `k8s.*` attributes | the collector's `k8s_attributes` pod association (see `otel-demo-values.yaml`) |
| `no node is in svc:otel-demo/` | the demo runs in another namespace | install OTel Demo into `otel-demo`: the namespace is part of every node id |
| `I1 … broken` / `I3 … broken` | a span or a call was lost or counted twice | a faultline bug; keep `tmp/demo/faultline.log` |
| `faultline restarted N times` | the pod restarted during the demo, the in-memory graph was lost | `kubectl … -n faultline describe pod`, often memory |

## Clean up

```sh
make demo-down
```
