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
3. Build `faultline:dev`, load it into kind (the node pulls `NATS_IMAGE` from `versions.env` by digest itself: `kind load` of a multi-platform image fails with the containerd image store), apply `deploy/dev/skeleton`, wait until NATS is ready, then restart faultline, run `faultline tokens init` in its pod (retried until the pod runs it; an error of the command itself, `faultline tokens: …`, stops the demo at once) and wait until `/readyz` answers 200 (up to 3 minutes: the pod waits for the token bucket, takes the partition lease, restores the checkpoint and creates the streams).
4. Issue an ingest token of the tenant `default` into the Secret `otel-demo/ingest-token` (key `token`, the token id in the annotation `ingest-token-id`) and wait until 30 s have passed since the issue, so that faultline accepts it.
5. Install OTel Demo (chart version from `versions.env`) with `deploy/dev/otel-demo-values.yaml` and `deploy/dev/otel-demo-faultline-values.yaml`, restart the collector DaemonSet (it reads `INGEST_TOKEN` from the Secret only at start), wait until every Deployment and the collector are ready, then revoke every other token of the tenant `default`, keeping the one whose id is in the Secret (a run that stopped half way leaves its token to this cleanup). The first run pulls about 25 images and takes 10–15 minutes.
6. Port-forward the faultline API to `127.0.0.1:$DEMO_PORT`, wait for `/readyz`, poll the graph every 10 s for up to 5 minutes until every required edge is there.
7. Wait for a `pipeline.stats` log record, then run the full check.

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
| `missing required edge: A -> B` | A call that happens in every run never paired | the collector reached faultline? `kubectl … -n otel-demo logs daemonset/otel-collector-agent` (`Unauthenticated` there: the Secret holds a revoked or foreign token, see [Tokens](#tokens)); `pipeline.stats` counters `expired`, `unpairable` |
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

1. The poll in step 6 waits, still for up to 5 minutes, until every required edge also has a **final** window with `requests > 0` and `latency_samples > 0`, and every service that receives a required edge has a final window of its own. With the default 10 s windows and 30 s watermark delay the first finals appear about 40 s after traffic starts.
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
| the token bucket is missing, or its group has no leader or no quorum | `/readyz` 503 in `waiting_tokens`, Warn `lifecycle.still_waiting{state=waiting_tokens}`; `faultline tokens init` ends the wait | waits until SIGTERM |
| the token bucket config lets grants in or hides them (`tokens.ErrBucketDrift`) | Error `lifecycle.start_failed` with the hint to run `faultline tokens init` and, if it reports that the bucket needs recreating, [the runbook](#tokens--recreate-the-bucket) | code 1 |
| the checkpoint cannot be restored | Error `checkpoint.restore_failed{hint="CHECKPOINT_RECOVERY=discard:p0:…"}` | code 1 |
| everything is up | Info `lifecycle.state{state=serving}`, `/readyz` 200 | — |

`/healthz` is the liveness probe and answers 200 while the process runs, including while it waits for NATS, the token bucket, the lease or the streams; `/readyz` is the readiness probe and answers 200 only while serving. Never put `/readyz` on a liveness or startup probe: a standby waits for the lease without a limit. If you add a startup probe on `/readyz` anyway, give it at least 80 s, the restore budget at the defaults.

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
- NATS keeps JetStream on the claim `nats-store` (6 Gi, `ReadWriteOnce`, the default storage class, `standard` in kind). Deleting or recreating the NATS pod (`Recreate`, an edit of `nats.conf`: `configMapGenerator` gives the ConfigMap a new hash and the NATS Deployment rolls) keeps the stream, the checkpoints, the lease and the token bucket: the new pod mounts the same claim. While NATS is away a running `all` cannot renew its lease: when that lasts past the 25 s lease (Warn `lease.suspended` 20 s after the last renewal) it logs Error `lease.lost` (a page), exits with code 1 and restarts from the last checkpoint. The claim goes away only with the namespace or the cluster (`make demo-down`). Production storage (three replicas) is step 11.
- Sizes: `PARTITIONS × (STREAM_MAX_BYTES + 4.4 × worst checkpoint image) + DLQ 1 GiB` = 2 GiB + 837.7 MiB + 1 GiB ≈ 3.8 GiB fits `max_file_store: 5GB`, which fits the 6 Gi claim. The checkpoint spool (`CHECKPOINT_DIR`) needs two worst images plus 64 MiB, 444.8 MiB for `all`; its volume has 512 Mi. `max_payload: 8MB` covers the largest record with its headers. `TestTheSkeletonNATSHoldsWhatTheAllRoleWrites` checks all four against the runtime loaders.

### Memory of `all`

The process refuses to start (code 2) when its worst case does not fit 0.85 × `GOMEMLIMIT` (1 632 MiB → 1 387.2 MiB). At the defaults `all` needs 1 340.1 MiB: OTLP requests 440.0, pairing, graph and traffic 402.1 (sketch arena 2 Mi entries), API responses 256.0, feed credit, pull and the message held 88.0, records waiting for PubAck 60.0, `Apply` 27.8, snapshots 23.5, OTLP connections 20.3 (4 connections, the flood of refused requests measured on them included), publish buffer 8.0, NATS connections 7.0, checkpoint 6.2, token directory 0.8 (two snapshots of 1 024 grants), admission tokens 0.4. Headroom is 47.1 MiB. Self-tracing (`SELF_TRACING_ENDPOINT`) takes about 2 MiB of it; the next ingest slices take theirs from the 11.1 MiB above the 36 MiB floor that `TestTheAllHeadroomIsPinned` keeps.

Raising `SKETCH_MAX_ENTRIES` or `PAIRING_MAX_PENDING` needs a larger `GOMEMLIMIT` (and the pod memory limit with it). A subject (a service or an edge) takes arena entries in proportion to its call rate: about 700 at 4 calls/s, 2 082 at 16 calls/s and 3 584 at saturation (64 calls/s and above: 7 live windows of a 512-entry block). The 2 Mi arena holds about 2 900 subjects at 4 calls/s and about 585 at saturation without losing a sample; beyond that the arena collapses buckets and then drops samples (`sketch.dropped_samples` in `pipeline.stats`). The graph limits (10 000 services, 50 000 edges) are not an arena size: at saturation they need about 1.6 GiB of arena. Step 11 sizes the processor arena from the measured entries per subject. The hint printed for a wrong `SKETCH_MAX_ENTRIES` names the processor default 8388608, not the `all` default 2097152 (known).

### Settings that depend on each other

- The ingester's `STREAM_DUPLICATE_WINDOW` must not be longer than the processor's. The ingester only checks the dedup window of the streams at start and waits in `waiting_streams` until it is long enough; the processor's `EnsureTopology` sets its own window, so a longer one on the ingester is a wait that never ends (visible as `still_waiting{state=waiting_streams}`). `all` uses one value for both.
- `NATS_TLS_CA_FILE` turns TLS on for every URL, `nats://` ones too (nats.go 1.56): set it only for a server that speaks TLS, otherwise the connection fails.
- `all` defaults `TOMBSTONE_TTL` to 15 s. Setting `PAIRING_TTL` above 15 s needs `TOMBSTONE_TTL` ≥ `PAIRING_TTL`, otherwise the start fails with code 2.

### Upgrading from the image before the `all` defaults changed

A checkpoint written by `all` with the earlier defaults (`TOMBSTONE_TTL=1m`, `SKETCH_MAX_ENTRIES=8388608`) does not restore with the new ones: the start logs Error `checkpoint.restore_failed` with a hint. Set the hinted `CHECKPOINT_RECOVERY=discard:p0:…` once, let the pod start with an empty graph, then remove the variable. Going back to the old values is not an option at the default `GOMEMLIMIT`: the 8 Mi arena does not fit the budget.

## Tokens

Every OTLP request needs `authorization: Bearer <ingest token>`. faultline checks it before it reads the body: no token, a wrong one, or a token of another tenant gets `UNAUTHENTICATED`; while the token directory is stale (no fresh check of the bucket for 45 s) every request gets `UNAVAILABLE` and the collector retries. The grants live in the NATS KV bucket `ingest_tokens` (stream `KV_ingest_tokens`) on the claim `nats-store`, so a restart of the NATS pod keeps them.

Run the command in the faultline pod (the binary is `/faultline`, it is not on `PATH`); it reads `NATS_URL`, `TENANT` and `STREAM_REPLICAS` from the pod:

```sh
kubectl --context kind-faultline-demo -n faultline exec deploy/faultline -- /faultline tokens init
(
  set -euo pipefail
  token="$(kubectl --context kind-faultline-demo -n faultline exec deploy/faultline -- /faultline tokens issue --tenant default --stdout)"
  printf '%s' "$token" |
    kubectl --context kind-faultline-demo -n otel-demo create secret generic ingest-token --from-file=token=/dev/stdin --dry-run=client -o yaml |
    kubectl --context kind-faultline-demo apply --server-side --field-manager=demo-1 --force-conflicts -f -
)
kubectl --context kind-faultline-demo -n faultline exec deploy/faultline -- /faultline tokens list
kubectl --context kind-faultline-demo -n faultline exec deploy/faultline -- /faultline tokens revoke <id>
```

- `init` creates the bucket or reconciles its config and prints `token bucket ready, grants: N`. Ingesters wait in `waiting_tokens` until it exists, in any order with `init`.
- `issue [--tenant <tenant>] (--output-file <path> | --stdout)`: `--tenant` wins over `TENANT` (default `default`). The id goes to stderr first (`issuing ingest token <id> for tenant <tenant>`), then the token once to stdout or to a new file with mode 0600. Without `-t` stdout is a pipe, so `issue` with neither flag refuses with code 2; `--stdout` into a file others can read is refused too, but through `kubectl exec` that check runs in the pod and does not see a redirect on your machine: put the token straight into the Secret as above, never into a file or the terminal. `$(…)` drops the line break `issue` ends the token with, and the subshell stops before the Secret when `issue` fails, so a failed issue never writes an empty token over the live one. The field manager is the one of `make demo-1`, which owns `data.token` after its first run; that Secret has no `ingest-token-id`, so the next `make demo-1` revokes this token with the other stale ones. Every ingester accepts the token within 22 s while NATS is reachable.
- `list` prints `id`, tenant and issue time (RFC 3339), tab-separated; never a secret or a digest.
- `revoke <id>`: every ingester refuses the token within 22 s while NATS is reachable, within 45 s when it is not (49 s when the ingester is cut off from the KV leader).
- Exit codes: 0 done, 1 failed (`faultline tokens: <error>` and a hint line on stderr), 2 a wrong command line or environment.

**Rotation.** `issue` a new token, wait at least 30 s, switch the client (update its Secret and restart it: the collector reads `INGEST_TOKEN` only at start), then `revoke` the old one. `make demo-1` does exactly this on every run for the tenant `default` and then revokes all other tokens of that tenant, keeping only the one whose id is in the Secret's annotation `ingest-token-id`; so a run that failed half way, or a revoke that failed, is cleaned up by the next run.

**When `issue` cannot tell what happened** (a write whose outcome is unknown, or a token that could not be shown or revoked), it exits 1 with `the token may be live: run faultline tokens revoke <id>, then faultline tokens list`. That `revoke`, and any second `revoke` of the same id, may itself exit 1 with `unknown or already revoked ingest token id`: the key is already gone (never written, or removed by the compensation), and a revoke cannot tell the two apart. The id missing from `list` is the answer: nothing accepts that token after 22 s.

**The bucket is not backed up.** Never restore it, or the NATS volume, from a backup or a snapshot: a restore brings back revoked tokens. A lost bucket is recreated and every token issued again (below).

**Alerts** (self-alerts in the faultline log):

| Alert | What it means | What to do |
|---|---|---|
| `auth.token_directory_stale` | no fresh check of the bucket for 45 s: every request gets `UNAVAILABLE` | Warn `tokens.check_failed{reason=drifted}` in the log: a field that lets grants in or hides them (`Subjects`, `Sources`, `MaxAge`, `MaxMsgs`, `MaxBytes`, …) differs from the reference, run `faultline tokens init`, and if it reports a recreate, [Tokens / recreate the bucket](#tokens--recreate-the-bucket); any other `reason`: NATS and the KV group leader |
| `auth.token_bucket_drift` | the bucket config differs from the reference in a write-contract field (`NoAck`, `Discard`, `Duplicates`, `RePublish`, `AllowRollup`, …); ingest goes on. Placement fields (`Replicas`, `Storage`, `DenyDelete`, …) raise no alert, only Warn `tokens.bucket_config_differs` at the first load | `faultline tokens init` |
| `auth.token_revoke_blocked` | the bucket denies purge or is sealed: `revoke` cannot work | [Tokens / recreate the bucket](#tokens--recreate-the-bucket), in a maintenance window |
| `auth.token_sequence_regressed` | the bucket went back in time within its epoch (a restore): the directory stays stale until the bucket is recreated | [Tokens / recreate the bucket](#tokens--recreate-the-bucket), now |
| `auth.token_epoch_changed` | the bucket was recreated; held for an hour | if you did not recreate it: [Tokens / recreate the bucket](#tokens--recreate-the-bucket) |
| `auth.token_mass_revoke` | between two reads more than half of the grants and more than 16 vanished, or at least two vanished and the bucket is left empty (a purge); held for an hour | who purged it; `faultline tokens list` |

**Known limits until the slices 3b-2 and 3b-4.** The tokens protect ingest fully only together with them:

- The bearer token travels in plain h2c, and any NATS client of the cluster can write to the bucket (3b-4: TLS and accounts).
- Warn `otlp.auth.rejected` and Warn `tokens.check_failed` are a sample: one Warn per reason per 10 s for all peers together, with `suppressed_requests` / `suppressed`; `peer` and `token_id` in it are an example, not the list. The counters (`auth` in `pipeline.stats`) are exact.
- `make demo-1` runs `kubectl exec deploy/faultline` right after `rollout restart`; with `strategy: Recreate` it may still reach the old pod, so the binary of the old image runs `tokens init`. That is harmless while both images share the reference bucket config; an image that changes it needs one more `faultline tokens init` after the rollout.
- A token shown on stdout ends on a signal even into a stalled pipe or a terminal stopped with Ctrl-S: the token is revoked and the command exits 1. `list` and the notes on stderr have no such limit: into a stalled pipe or a stopped terminal they wait, and only SIGKILL ends them (no grant is at stake).

### Tokens / recreate the bucket

Hints of `faultline` and `faultline tokens` name this section. Follow it on `ErrNeedsRecreate`, `ErrIrreversible`, the alerts `auth.token_sequence_regressed` and `auth.token_epoch_changed` (when you did not recreate the bucket), and whenever the bucket or the NATS volume was restored from a backup:

1. Do not restart the ingesters until `faultline tokens init` in step 4 has run: a stale directory answers `UNAVAILABLE` and the collectors retry, while a new process would take the restored state as the first epoch.
2. If the cause is only `DenyPurge` or `Sealed` and no token is compromised, ingest works: recreate in a maintenance window.
3. `faultline tokens list`: save the tenants and the ids.
4. With the account of `faultline tokens` (only it may delete this stream; in the dev skeleton there are no accounts): `nats stream rm KV_ingest_tokens`, then `faultline tokens init`, `faultline tokens issue` for each tenant, and update the collectors' Secrets. In kind: `kubectl --context kind-faultline-demo -n faultline port-forward svc/nats 4222:4222 &` and `nats -s nats://127.0.0.1:4222 stream rm KV_ingest_tokens -f`; `make demo-1` then issues the demo token again.
5. Until their Secrets are updated, collectors with the old tokens get `UNAUTHENTICATED` and lose spans: hence the maintenance window.

## Clean up

```sh
make demo-down
```
