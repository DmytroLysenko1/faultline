# Step 1 demo: OTel Demo → faultline → service graph

`make demo-1` runs the OpenTelemetry Demo in a local kind cluster, sends its traces to faultline and checks the graph faultline builds against the reference edges in `test/demo/otel-demo/`.

## Before you start

- Docker with at least **8 GB** of VM memory (Docker Desktop: Settings → Resources; OTel Demo alone needs about 7 GiB, faultline adds up to 2 GiB and NATS up to 512 MiB).
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
3. Build `faultline:dev`, load it into kind (the node pulls `NATS_IMAGE` from `versions.env` by digest itself: `kind load` of a multi-platform image fails with the containerd image store), apply `deploy/dev/skeleton`, wait until NATS is ready, then restart faultline and wait until `/readyz` answers 200 (up to 3 minutes: the pod takes the partition lease, restores the checkpoint and creates the streams).
4. Install OTel Demo (chart version from `versions.env`) with `deploy/dev/otel-demo-values.yaml` and `deploy/dev/otel-demo-faultline-values.yaml`, wait until every Deployment and the collector DaemonSet are ready. The first run pulls about 25 images and takes 10–15 minutes.
5. Port-forward the faultline API to `127.0.0.1:$DEMO_PORT`, wait for `/readyz`, poll the graph every 10 s for up to 5 minutes until every required edge is there.
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
| `faultline restarted N times` | the pod restarted during the demo; the graph comes back from the last checkpoint, but what arrived after it is replayed from the stream and the check cannot tell | `kubectl … -n faultline describe pod`, often memory |

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
| `cardinality.services_over_limit != 0` | a service has more than 500 operations, new ones go to `__overflow__` | the `windows.cardinality_limit_reached` warning names the services; add a route rule (`ROUTE_RULES_FILE`) |
| `windows.subjects.offered == 0` | the last `pipeline.stats` line counted no events at all | the collector reaches faultline? `ingest.accepted_spans` |
| `cardinality.operations == 0` | no operation is held: no traffic in the open windows when the line was written, or the spans carry no `http.route`, `url.path` or `rpc.method` | `windows.operations.offered`; the span attributes |
| `… missing in pipeline.stats` | the log comes from a faultline without step 2 | rebuild the image: `make image kind-load` |
| required edges of the checkout path (`frontend -> checkout`, `checkout -> email`) missing after Docker Desktop restarted | the demo pods came back from the VM restart and checkout stopped finishing orders (the `email` log shows no new orders) | `kubectl --context kind-faultline-demo -n otel-demo rollout restart deployment` and run the demo again |

## Operating faultline on NATS

The skeleton runs one `faultline all` pod (the image entrypoint) and one NATS pod with JetStream, both `strategy: Recreate` with one replica. `all` is the ingester and the processor in one process: OTLP is written to the partition stream `SPANS_P0`, the processor reads it back into the graph and checkpoints the graph into NATS.

### Start: what waits and what fails

| Situation | What you see | Exit |
|---|---|---|
| a variable is wrong or missing (`NATS_URL`, `CHECKPOINT_DIR`, sizes, durations) | one line on stderr naming the variable, the pod restarts | code 2 |
| NATS is not up yet | Warn `nats.unreachable{reason=dns\|…}` at once, then Warn `lifecycle.still_waiting{state=connecting}` every 30 s, the first one 30 s after the start; `/readyz` 503 | waits until SIGTERM |
| another pod holds the partition lease | `/readyz` 503 in `waiting_lease`; the old pod's lease expires within 25 s if it was killed | waits until SIGTERM |
| the streams are missing or being repaired | Warn `lifecycle.still_waiting{state=waiting_streams}` | waits until SIGTERM |
| the checkpoint cannot be restored | Error `checkpoint.restore_failed{hint="CHECKPOINT_RECOVERY=discard:p0:…"}` | code 1 |
| everything is up | Info `lifecycle.state{state=serving}`, `/readyz` 200 | — |

`/healthz` is the liveness probe and answers 200 while the process runs, including while it waits for NATS, the lease or the streams; `/readyz` is the readiness probe and answers 200 only while serving. Never put `/readyz` on a liveness or startup probe: a standby waits for the lease without a limit. If you add a startup probe on `/readyz` anyway, give it at least 80 s, the restore budget at the defaults.

### Alerts

| Rule | Severity | Why |
|---|---|---|
| any line with `level=ERROR` | page | one Error per run names the incident |
| `msg ∈ {lifecycle.stop_failed, lease.lost} ∧ followup=true` (Warn) | ticket | a stop failure or a lease loss next to an incident that already paged: one page, one ticket for the leak; ship this rule together with the page rule |
| Warn `otlp.export.overloaded` | ticket | the stream is full or the journal is slow: senders get `RESOURCE_EXHAUSTED` with `RetryInfo` |
| Warn `lifecycle.still_waiting{state=connecting}` or `{state=waiting_streams}` lasting minutes | ticket | NATS is down or JetStream is out of storage |
| Warn `lifecycle.forced_stop` | none | a normal outcome: a server closed by force whose goroutine exited, or unflushed tracing |

A late Error `lease.lost` can arrive up to about 10 s after the partition stopped (the layers above it stop first); it is the same incident, no extra rule.

There is no ticket rule for Warn `lifecycle.component_exit{followup=true}`: it is a layer stopping after an Error that already paged, and no resource leaks behind it.

### Stop: grace 25 s and no preStop

SIGTERM stops the layers in reverse: OTLP, journal, the partition (drain, final checkpoint, lease release), stats, NATS connection, HTTP, tracing. Their budgets add up to 21.5 s at `PUBLISH_TIMEOUT=3s` (20.5 s at the default 2 s); with 2 s for the kubelet that is 23.5 s within `terminationGracePeriodSeconds: 25`. `TestStopBudgetOfEveryRoleFitsItsGracePeriod` reads the grace from `deployment.yaml` and fails if they stop fitting. There is no `preStop`: with `Recreate` and one replica nothing routes traffic to a terminating pod, and every second of `preStop` comes out of the same 25 s, so a `sleep 5` would need 28.5 s. When the ingester and the processor get their own Deployments, each gets grace 20.

### Full stream, restarts and what is lost

- The stream keeps up to `STREAM_MAX_BYTES` (2 GiB) and refuses new messages when full. The exporter gets `RESOURCE_EXHAUSTED` with `RetryInfo` and retries. A span whose export got OK is in the stream and is not lost by any restart of the faultline pod.
- A faultline pod restart restores the graph from the last checkpoint and replays the stream from there.
- A processor killed by `SIGKILL` at any moment (while it uploads a checkpoint, moves the checkpoint pointer, purges the stream below it, restores, or reads the stream) comes back to the same graph as a run that was never killed: the pointer only moves to a complete object, the stream is purged only below a committed pointer, and the restart reads from the pointer position. A lost answer (the write was done, the process never learned it) is the same as a kill after the write. The next pod waits for the 25 s lease of the killed one. `TestAKilledProcessorRestartsToTheSameGraphAsAnUninterruptedRun` kills the binary at 7 fixed points and 3 random ones — after the k-th record delivered from the stream or the k-th answered checkpoint write (`TEST_KILL_SEED` replays a run); the windows, the waiting halves of calls and the counts are all in the checkpoint it restarts from, and the graph without kills is checked against the records. `make test-stress` runs 10 random points with the real lease TTL, `TestAKilledIngesterLosesNoAcknowledgedRequest` (every export answered OK before a `SIGKILL` of the ingester is in the graph) and `TestALeaseLostWhileServingStopsWithoutRelease` (a processor whose lease was taken exits with code 1 and leaves the new holder's key).
- NATS keeps JetStream on an `emptyDir`. A container restart keeps it; deleting or recreating the NATS pod (`Recreate`, a node drain) wipes the stream, the checkpoints and the lease. A running `all` cannot renew its lease on the new server: after about 26 s (Warn `lease.suspended` 20 s after the last renewal, the loss when the 25 s lease runs out, then the stop) it logs Error `lease.lost` (a page) and exits with code 1; the pod restarts once and starts with an empty graph. Any edit of `nats.conf` does the same: `configMapGenerator` gives the ConfigMap a new hash, the NATS Deployment rolls, and the new pod starts on an empty volume. Fine for dev; production storage (PVC, three replicas) is step 11.
- Sizes: `PARTITIONS × (STREAM_MAX_BYTES + 4.4 × worst checkpoint image) + DLQ 1 GiB` = 2 GiB + 837.7 MiB + 1 GiB ≈ 3.8 GiB fits `max_file_store: 5GB`, which fits the 6 Gi volume. The checkpoint spool (`CHECKPOINT_DIR`) needs two worst images plus 64 MiB, 444.8 MiB for `all`; its volume has 512 Mi. `max_payload: 8MB` covers the largest record with its headers. `TestTheSkeletonNATSHoldsWhatTheAllRoleWrites` checks all four against the runtime loaders.

### Memory of `all`

The process refuses to start (code 2) when its worst case does not fit 0.85 × `GOMEMLIMIT` (1 600 MiB → 1 360 MiB). At the defaults `all` needs 1 318.5 MiB: OTLP requests 440.0, pairing, graph and traffic 402.1 (sketch arena 2 Mi entries), API responses 256.0, feed credit, pull and the message held 88.0, records waiting for PubAck 60.0, `Apply` 27.8, snapshots 23.5, publish buffer 8.0, NATS connections 7.0, checkpoint 6.2. Headroom is 41.5 MiB. Self-tracing (`SELF_TRACING_ENDPOINT`) and the token directory of a later step take about 8 MiB of it.

Raising `SKETCH_MAX_ENTRIES` or `PAIRING_MAX_PENDING` needs a larger `GOMEMLIMIT` (and the pod memory limit with it). The 2 Mi arena holds about 2 500 services and edges with latencies spread over the full range without losing a sample; beyond that the arena collapses buckets and then drops samples (`sketch.dropped_samples` in `pipeline.stats`). The hint printed for a wrong `SKETCH_MAX_ENTRIES` names the processor default 8388608, not the `all` default 2097152 (known).

### Settings that depend on each other

- The ingester's `STREAM_DUPLICATE_WINDOW` must not be longer than the processor's. The ingester only checks the dedup window of the streams at start and waits in `waiting_streams` until it is long enough; the processor's `EnsureTopology` sets its own window, so a longer one on the ingester is a wait that never ends (visible as `still_waiting{state=waiting_streams}`). `all` uses one value for both.
- `all` defaults `TOMBSTONE_TTL` to 15 s. Setting `PAIRING_TTL` above 15 s needs `TOMBSTONE_TTL` ≥ `PAIRING_TTL`, otherwise the start fails with code 2.

### Upgrading from the image before the `all` defaults changed

A checkpoint written by `all` with the earlier defaults (`TOMBSTONE_TTL=1m`, `SKETCH_MAX_ENTRIES=8388608`) does not restore with the new ones: the start logs Error `checkpoint.restore_failed` with a hint. Set the hinted `CHECKPOINT_RECOVERY=discard:p0:…` once, let the pod start with an empty graph, then remove the variable. Going back to the old values is not an option at the default `GOMEMLIMIT`: the 8 Mi arena does not fit the budget.

## Clean up

```sh
make demo-down
```
