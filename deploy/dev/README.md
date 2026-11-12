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
grpcurl -plaintext -max-msg-sz 201326592 127.0.0.1:18080 faultline.v1.GraphService/GetGraph
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

## Step 2 demo: windows and RED

```sh
make demo-2
```

The same steps as `make demo-1`, logged as `demo-2: …`, with two stricter checks (`democheck -red`):

1. The poll in step 5 waits, still for up to 5 minutes, until every required edge also has a **final** window with `requests > 0` and `latency_samples > 0`, and every service that receives a required edge has a final window of its own. With the default 10 s windows and 30 s watermark delay the first finals appear about 40 s after traffic starts.
2. The full check also reads the last `pipeline.stats` line and checks, from that one line:
   - I6 — for each engine (`subjects`, `operations`, `overflow`): `offered = on_time + late_accepted + late_dropped + no_watermark`;
   - I10 — `graph.calls_total + graph.served_total = windows.subjects.offered`;
   - I11 — `operations.offered + overflow.offered = subjects.offered`, `operations.late_dropped = subjects.late_dropped`, `operations.series ≤ operations.capacity`;
   - `windows.subjects.offered > 0`, `subjects.late_dropped / offered < 1 %`, `cardinality.services_over_limit = 0`, `cardinality.operations > 0`;
   - a missing counter fails instead of reading as zero, and sums that would overflow fail too.

   A group missing from the line fails the check instead of skipping it: a step 1 image cannot pass `demo-2`.

A passing run ends with:

```
15/15 required, 2 optional, 17 edges, I1 ok, I3 skipped
RED: finals on 15/15 required edges, 12/12 receivers, 11/11 stats checks
demo-2: demo passed; graph in tmp/demo/graph.json
```

Look at the RED numbers:

```sh
tmp/bin/faultctl graph export -o json --server http://127.0.0.1:18080 \
  | jq '.edges[] | {source_index, target_index, requests: .final.requests, failed: .final.failed_requests, p95: .final.p95}'
tmp/bin/faultctl graph export -o dot --server http://127.0.0.1:18080 | dot -Tsvg > graph.svg
```

In DOT an edge reads `calls N, failed M` and under it `R/s err E% p95 D` of its last final window; a line starting with `~` comes from the still-open window. A node reads `served …` the same way. `grpcurl` needs the `-max-msg-sz 201326592` shown above for a graph at the limits (192 MiB send limit).

`make demo-check DEMO_CHECK_FLAGS=-red` re-runs the step 2 check on the saved graph and log.

| Output | What it means | What to look at |
|---|---|---|
| `required edge without a final window with latency: A -> B` | the edge exists but no window of it has closed with latency yet | wait longer; the watermark in `pipeline.stats` (`windows.watermark_lag_seconds`) moves? |
| `receiver without a final window: B` | B received calls but none of its server spans landed in a closed window | `windows.subjects.no_watermark`, `late_dropped` |
| `I6 … != …`, `I10 … != …`, `I11 … != …` | an event was lost or counted twice between the engines | a faultline bug; keep `tmp/demo/faultline.log` |
| `subjects.late_dropped/offered >= 1%` | too many events arrived after their window retired | collector batching or clock skew; `windows.subjects.clock_skew_reassigned`, the `windows.late_drop_ratio_high` warning |
| `cardinality.services_over_limit != 0` | a service has more than 500 operations, new ones go to `__overflow__` | the `windows.cardinality_limit_reached` warning names the services; add a route rule (`FAULTLINE_ROUTE_RULES_FILE`) |
| `windows.subjects.offered == 0` | the last `pipeline.stats` line counted no events at all | the collector reaches faultline? `ingest.accepted_spans` |
| `cardinality.operations == 0` | no operation is held: no traffic in the open windows when the line was written, or the spans carry no `http.route`, `url.path` or `rpc.method` | `windows.operations.offered`; the span attributes |
| `… missing in pipeline.stats` | the log comes from a faultline without step 2 | rebuild the image: `make image kind-load` |
| required edges of the checkout path (`frontend -> checkout`, `checkout -> email`) missing after Docker Desktop restarted | the demo pods came back from the VM restart and checkout stopped finishing orders (the `email` log shows no new orders) | `kubectl --context kind-faultline-demo -n otel-demo rollout restart deployment` and run the demo again |

## Clean up

```sh
make demo-down
```
