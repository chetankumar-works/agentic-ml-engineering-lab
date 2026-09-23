# Mistakes & Resolutions

A short, scannable log of things that went wrong during this build and
how they were actually fixed — not a sanitized story, the real one. Full
narrative context for each lives in `LEARNING_LOG.md` (the teaching
writeup) and `RUNBOOKS.md` (the operational runbook); this file exists so
a reader can scan "what broke → what fixed it" in under a minute per
entry without reading either of those in full.

Format per entry: **What happened** → **Root cause** → **Fix** →
**Where to read more**.

---

## Milestone 0 — Environment

### System Python was too new for the project's dependencies
- **What happened**: the host's default `python3` was 3.14.4; several
  planned dependencies (Airflow, Feast, Kubeflow SDK, Pandera, MLflow)
  weren't guaranteed to publish `cp314` wheels yet.
- **Root cause**: a brand-new CPython release always outpaces the
  data/ML tooling ecosystem's binary wheel support by months.
- **Fix**: pinned the project to Python 3.12 via `uv python install`
  (a prebuilt, standalone interpreter — no compiler toolchain needed,
  which mattered because this host also has no passwordless `sudo` and
  is missing the build-essential/`libssl-dev` packages `pyenv` would
  have needed).
- **More**: `DECISIONS.md` ADR-0001.

---

## Milestone 1 — Kafka + PostgreSQL streaming ingestion

### The UCI HIGGS dataset server silently stalled for over an hour
- **What happened**: a background download of the ~2.8GB dataset sat at
  a fixed byte count for 60+ minutes with no error.
- **Root cause**: `archive.ics.uci.edu` is a slow, unmonitored academic
  server with no stall-timeout behavior on the client side to catch it.
- **Fix**: killed and restarted the download with `curl
  --speed-time/--speed-limit` (abort if throughput drops below a
  threshold for too long), so a future stall fails fast instead of
  hanging silently.

### The same download's connection was reset mid-transfer at ~1.3GB
- **What happened**: a second attempt died with `curl: (18) transfer
  closed with outstanding read data remaining` after transferring 1.3GB.
- **Root cause**: an unreliable network path to the source server;
  `curl`'s default `--retry` doesn't retry every error class.
- **Fix**: `curl --retry-all-errors --retry 15` on the final successful
  attempt, which retries genuinely any failure, not just the default
  "safe" transient ones.

### The server rejects HTTP Range/resume requests outright
- **What happened**: an attempted resumed download (`curl -C -`) failed
  immediately, every time, with `curl: (33) HTTP server does not seem to
  support byte ranges`.
- **Root cause**: the server doesn't support partial-content responses
  at all (confirmed by manually sending a `Range` header and getting
  `200 OK` with the full body back, not `206 Partial Content`).
- **Fix**: gave up on resuming; every retry re-downloads from byte zero.
  Slower, but the only option this server supports.

### The real dataset archive didn't match the assumed format
- **What happened**: `dataset.py`'s zip-reading code, written and
  tested against a hand-built `HIGGS.csv` fixture, failed to find any
  `.csv` member once run against the real downloaded file.
- **Root cause**: the actual UCI archive contains `HIGGS.csv.gz` — the
  CSV is gzip-compressed *inside* the zip, not stored plain. The
  assumption came from the dataset's documentation page, not from
  inspecting the real file.
- **Fix**: `_find_csv_member`/`stream_rows` now handle both a plain
  `.csv` and a `.csv.gz` member (gunzip-wrapping the zip member's stream
  when needed), with a regression test built from a synthetic
  gzip-inside-zip fixture so this can't silently regress.
- **Lesson**: a unit test against a hand-built fixture only proves the
  code handles what you *assumed* the format was — it takes a real run
  against the real file to catch an assumption that was simply wrong.

---

## Milestone 2 — Airflow bronze/silver/gold batch pipeline

### MinIO's Docker Hub images no longer exist
- **What happened**: `docker compose up` failed with `pull access denied
  for minio/mc, repository does not exist or may require 'docker login'`.
- **Root cause**: MinIO moved its container images off Docker Hub to
  Quay.io (a 2024/2025 licensing-driven change); `minio/minio` and
  `minio/mc` are stale references now.
- **Fix**: switched to `quay.io/minio/minio` and `quay.io/minio/mc`.

### A host port collision with an unrelated project's containers
- **What happened**: MinIO's container failed to start with `Bind for
  127.0.0.1:9000 failed: port is already allocated`.
- **Root cause**: another, unrelated Docker Compose project on the same
  host had a container using port 9000/9001 (not visibly bound per
  `docker ps`, but still conflicting at the kernel level).
- **Fix**: remapped AMEL's MinIO to host ports 9010/9011 — the
  in-network container-to-container port (9000) is unaffected, only the
  host-published mapping changed.

### A manually-triggered Airflow DAG run has no `data_interval_end` at all
- **What happened**: `determine_high_watermark_task` crashed with
  `KeyError: 'data_interval_end'` the moment it was triggered ad hoc
  (`airflow dags trigger`) instead of by the schedule.
- **Root cause**: `data_interval_end`/`data_interval_start` only exist
  for schedule-driven runs; a manual trigger with no explicit logical
  date has no data interval — the key is absent, not `None`.
- **Fix**: added an `_upper_bound(context)` helper that falls back to
  `dag_run.run_after` (the run's actual trigger time) when no data
  interval exists.
- **Lesson**: exercise the code path an operator will actually use
  (`airflow dags trigger` for an ad-hoc run or backfill), not only the
  path the scheduler exercises automatically.

### A named Docker volume mounted as root, but the container runs as a non-root user
- **What happened**: `extract_new_records_task` crashed with
  `PermissionError: [Errno 13] Permission denied:
  '/opt/airflow/staging/...'`.
- **Root cause**: Docker gives a freshly created named volume root
  ownership by default; the Airflow container runs as the non-root
  `airflow` user and couldn't write into it.
- **Fix**: pre-created `/opt/airflow/staging` with the right ownership
  *inside the image* (`RUN mkdir -p /opt/airflow/staging` while
  `USER airflow` is active) — Docker copies a volume mount point's
  initial ownership from the image the first time it's populated, so the
  volume inherits `airflow:root` instead of defaulting to root.

### A bulk upsert exceeded Postgres's hard parameter limit
- **What happened**: `update_curated_tables_task` failed with
  `psycopg.OperationalError: ... number of parameters must be between 0
  and 65535` once a real backlog (~48k rows) had accumulated.
- **Root cause**: a single multi-row `INSERT ... VALUES (...), (...)`
  built from one row per extracted record scales parameter count as
  `rows × columns` — at 48k rows × 6 columns that's already ~288k,
  4x over Postgres's hard cap, regardless of server resources.
- **Fix**: chunked every bulk upsert at `UPSERT_BATCH_SIZE = 2000` rows.
- **Lesson**: this only shows up at real backlog scale — a small local
  dev test with a handful of rows would never hit it, which is exactly
  why this project's Definition of Done requires running against real,
  growing data, not just unit tests.

### The watermark could silently regress under concurrent DAG run retries
- **What happened**: several DAG runs ended up `up_for_retry`
  simultaneously (an artifact of iterating on the two bugs above live
  against a running scheduler) and were all retried together after an
  image rebuild. The stored watermark ended up sitting at an *earlier*
  run's `upper_bound` (`20:10:00`) even though later runs
  (`20:13:19`, `20:15:00`) had already completed successfully.
- **Root cause**: `advance_watermark`'s original `ON CONFLICT DO UPDATE
  SET watermark = <new value>` overwrites unconditionally — whichever
  transaction *commits last* wins, which is not necessarily the run with
  the logically latest `upper_bound` when runs overlap.
- **Fix**: made the upsert monotonic — `SET watermark =
  GREATEST(current, new)`. Verified by triggering a fresh run after the
  fix and confirming the watermark advanced from the regressed
  `20:10:00` to `20:25:00`, and that the resulting wide re-extraction
  window (re-processing already-curated data) produced **zero duplicate
  rows** in any curated table.
- **Lesson**: idempotent data writes don't automatically make *metadata*
  about those writes (a checkpoint, a watermark) correct under
  concurrency — the checkpoint itself needs its own correctness argument.
  The fact that this bug was low-severity (wasted reprocessing, not data
  loss) is *because* the upserts it gates were already idempotent — the
  two properties are complementary, not the same thing.

### A `SET` clause silently never fired
- **What happened**: `control.pipeline_watermarks.updated_at` stayed
  frozen at its first-ever insert timestamp even after multiple
  successful watermark advances.
- **Root cause**: the SQLAlchemy model's `onupdate=func.now()` default
  only fires through the ORM's unit-of-work (`session.add()` +
  `commit()`); it does nothing for a raw Core `INSERT ... ON CONFLICT DO
  UPDATE` executed via `session.execute(stmt)`.
- **Fix**: set `updated_at` explicitly in the same `SET` clause as the
  `GREATEST` watermark update.

- **More on all of the above**: `LEARNING_LOG.md`'s Milestone 2 entry
  (full data-flow/idempotency argument) and `RUNBOOKS.md` (operational
  runbook entries with diagnosis steps for each).

## Milestone 3 — Feast + Redis feature store

### `feast materialize` used 12 GB and was OOM-killed (and had frozen WSL2 the day before)
- **What happened**: the first Milestone 3 session ended with WSL2 frozen
  mid-`feast materialize`. On the retry, with 13 GB free, the kernel OOM
  killer took the `feast` process at 12.1 GB RSS instead.
- **Root cause**: Feast's Postgres offline store materializes a window by
  loading every row into memory and converting each into protobufs —
  ~14 KB/row × 885k rows. `materialize-incremental` with no prior
  materialization and a 10-year TTL means the window is the whole table.
  The 18 unrelated containers running the day before removed the
  headroom that would have let it fail cleanly.
- **Fix**: chunked materialization (`ml/feature_repo/scripts/materialize.py`,
  ≤25k rows per window, boundaries computed in SQL; measured peak
  875 MiB) and `mem_limit` on every Feast container.
- **Lesson**: "materialize" is a batch job with memory proportional to
  its input; treat it like one. And cap containers — a limit turns a
  VM-wide outage into a container exit code.

### The simulator's health endpoint said `ok` on a dead service for an hour
- **What happened**: after a broker stall the simulator's publishing
  thread died on an uncaught exception; `/health` returned 200, Compose
  showed `healthy`, and every downstream symptom (frozen landing tables)
  was initially misattributed to the id-replay problem below.
- **Root cause**: `/health` returned a constant; the producer's
  `on_delivery_error` hook existed but was wired to `None`; a daemon
  thread dying is silent by default.
- **Fix**: delivery reports feed a `ProducerHealth` state; `/ready` fails
  on current delivery errors, `/health` fails on loop death or a fatal
  producer error; the loop survives transient publish errors. Regression:
  `make failure-simulator-wedge`.
- **Lesson**: a probe that cannot fail is worse than no probe — it
  actively hides the outage. Every liveness/readiness endpoint must be
  derived from the thing it claims to report on.

### The ingestor died the same way, for a different exception
- **What happened**: Kafka evicted the ingestor from its consumer group
  during the stall; the next `commit()` raised `UNKNOWN_MEMBER_ID`,
  killed the loop, and `/health` stayed 200 with ~870k lag and no group
  members.
- **Root cause**: as above, plus treating a *rejected commit* as fatal
  when the design already makes redelivery safe (idempotent sink).
- **Fix**: catch `KafkaException` on commit → log, surface via `/ready`,
  continue (rejoin + redeliver); any other loop death → `/health` 503.
- **Lesson**: "at-least-once + idempotent sink" means a failed commit is
  a *retry*, not a crash. The Milestone 1 docstring even said a DB
  failure should "stop the process" — it stopped a thread and left the
  process (and its health check) running. Check what "stop" actually
  does.

### Restarting the simulator replayed 885k already-seen ids
- **What happened**: after the crash-restart, the ingestor logged
  `duplicate_events_skipped` for everything and no new curated rows
  appeared; at 100 events/s that is ~2.5 h before any new id.
- **Root cause**: `entity_id = f"higgs-{index}"` from `enumerate()`
  starting at 0 on every boot.
- **Fix**: `HIGGS_START_INDEX` (env → `Settings`), set to `max(landed)+1`
  on restart (RUNBOOKS.md).
- **Lesson**: a deterministic id generator is great for idempotency
  testing and terrible for restarts unless its position is externalized.

### Feast's Postgres offline store demanded SSL from a Postgres without it
- **What happened**: first live materialize failed with `server does not
  support SSL, but SSL was required`.
- **Root cause**: Feast defaults `sslmode=require` for its Postgres
  offline store.
- **Fix**: `sslmode: disable` in `feature_store.yaml`, with a comment.
- **Lesson**: read the defaults of every store config block; the
  registry URL and the offline store use different client libraries with
  different defaults.

## Milestone 4 — Training + MLflow

### The MLflow server booted straight into its memory cap
- **What happened**: `amel-mlflow-1` showed 1,023 MiB / 1 GiB seconds
  after starting, doing nothing.
- **Root cause**: MLflow 3's default topology — 4 gunicorn workers plus
  a job runner and two huey consumers for a server-side jobs feature.
- **Fix**: `--workers=2`, `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`
  → 483 MiB.
- **Lesson**: measure a new always-on container *before* wiring anything
  to it. "Default" is sized for someone else's machine.

### MLflow rejected every request with 403 "possible DNS rebinding attack"
- **What happened**: the first training run failed on
  `set_experiment`.
- **Root cause**: MLflow 3 validates the `Host` header; the Compose
  service name isn't in its default allow-list. The fix flag
  (`--allowed-hosts`) then errored because it is uvicorn-only and the
  command also passed `--gunicorn-opts`.
- **Fix**: `--allowed-hosts=mlflow:5000,localhost:5000,127.0.0.1:5000`,
  drop the gunicorn option.
- **Lesson**: the kickoff rule "inspect the current docs when a
  dependency version changed" — MLflow 3 is a different server than 2.

### `log_model` refused the decision tree as an untrusted type
- **What happened**: `UntrustedTypesFoundException:
  ['sklearn.tree._tree.Tree']`.
- **Root cause**: MLflow 3 serializes sklearn with skops, which audits
  types; a tree's node storage can be crafted to crash a loader.
- **Fix**: `skops_trusted_types=["sklearn.tree._tree.Tree"]` — one
  type, stored in the MLmodel flavor config for loaders.
- **Lesson**: prefer the safer serializer and declare the narrow
  exception, rather than falling back to pickle.

### Blank env vars from Compose broke config parsing
- **What happened**: `make promote` crashed in `TrainingConfig()`:
  `as_of: input is too short`.
- **Root cause**: `${TRAINING_AS_OF:-}` renders as `""`, which pydantic
  parses as a (bad) datetime, not as unset.
- **Fix**: a `mode="before"` validator maps blank strings to `None`;
  unit-tested.
- **Lesson**: Compose's "unset → empty string" is not Python's "unset →
  None"; treat every optional env var at the boundary.

### The new `mlflow` database didn't exist
- **What happened**: the init script was there; the database wasn't.
- **Root cause**: Postgres init scripts only run on a fresh data volume.
- **Fix**: created it by hand; runbook entry added.
- **Lesson**: anything under `docker-entrypoint-initdb.d` is
  first-boot-only — plan the manual step for existing volumes.

## Milestone 5 — FastAPI inference

### A FastAPI dependency silently became a required query parameter
- **What happened**: every `/predict/*` test returned 422 `query.tid:
  Field required`.
- **Root cause**: `from __future__ import annotations` makes annotations
  strings; FastAPI evaluates `Annotated[str, Depends(trace_id)]` against
  the module's globals, and `trace_id` was a closure inside
  `create_app` — so the annotation resolved to a plain `str` query param.
- **Fix**: module-level dependency function (comment left in place).
- **Lesson**: with postponed annotations, anything referenced inside an
  annotation must be importable at module scope.

### A refresh during an MLflow outage blocked for minutes
- **What happened**: `POST /model/refresh` with MLflow stopped hung
  ~3 minutes before returning the error.
- **Root cause**: MLflow's REST client retries 7× with exponential
  backoff by default.
- **Fix**: `MLFLOW_HTTP_REQUEST_MAX_RETRIES=2`,
  `MLFLOW_HTTP_REQUEST_TIMEOUT=10` for the API (20 s measured). Also:
  a successful no-op refresh now clears `last_error`, which had been
  left stale from the failed attempt.
- **Lesson**: a cached-model design only pays off if the *refresh* path
  also fails fast; client library defaults assume you want to wait.

## Milestone 6 — OpenTelemetry and observability

### Tempo 3 refused the retention config
- **What happened**: `field compactor not found`, then `field
  block_retention not found`.
- **Root cause**: Tempo 3.x moved compaction/retention to the
  backend-scheduler; the YAML keys from every 2.x example are gone.
- **Fix**: `-backend-scheduler.provider.work.compaction.block-retention=24h`
  as a CLI flag.
- **Lesson**: `latest` means "read this version's flags"; `--help` on
  the binary is the source of truth.

### Airflow's traces never arrived (HTTP/2 "Expected SETTINGS frame")
- **What happened**: hundreds of `Transient error StatusCode.UNAVAILABLE
  ... Trying to connect an http1.x server` warnings.
- **Root cause**: Airflow 3.3 loads the gRPC exporter regardless of
  `OTEL_EXPORTER_OTLP_PROTOCOL`; the endpoint was the collector's HTTP
  port.
- **Fix**: `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317`.
- **Lesson**: "OTLP" is two wire protocols; check which one a client
  actually speaks.

### The trace id in the response was not the trace id in Tempo
- **What happened**: Loki query for the API's `X-Trace-Id` found
  nothing; Tempo had the trace under a different id.
- **Root cause**: Milestone 5's `trace_id()` minted a uuid; the OTel
  instrumentation minted its own trace id; the log processor kept the
  explicit one.
- **Fix**: derive `X-Trace-Id` from the active span first.
- **Lesson**: one id, one source. Anything that mints ids must defer to
  the tracer once tracing exists.

### Expected one trace per Kafka message
- **What happened**: the simulator's `send` span and the ingestor's
  `recv` span were separate traces.
- **Root cause**: OTel messaging semantics — batch consumers *link* to
  producer spans rather than parent under them.
- **Fix**: none needed; documented, and `ingest_batch` spans added.
- **Lesson**: read the semantic conventions before declaring a bug.

## Milestone 7 — Docker hardening + CI/CD

### On a clean checkout the DAG never ran
- **What happened**: `airflow dags trigger` returned a run id that stayed
  `queued`; 0/10 tasks.
- **Root cause**: Airflow pauses newly discovered DAGs by default; the
  Milestone 2 stack had been unpaused by hand in the UI and the volume
  remembered it.
- **Fix**: `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false`.
- **Lesson**: any state that lives in a volume can hide a missing
  configuration; only a fresh-volume run finds it.

### Stale GitHub Action pins failed the first two CI runs
- **What happened**: `Unable to resolve action aquasecurity/trivy-action@0.28.0`,
  then `astral-sh/setup-uv@v10`.
- **Root cause**: guessed versions; `setup-uv`'s floating major is `v7`
  while its releases are `v10.x`.
- **Fix**: pinned from each repo's tag list (`gh api .../tags`).
- **Lesson**: look up action versions the same way as any dependency.

### Trivy flagged CVEs in code we never ship
- **What happened**: 4 CRITICAL findings, all in
  `.venv/.../feast/ui/yarn.lock`.
- **Root cause**: the scan walked the virtualenv; Feast bundles a JS UI
  lockfile we do not run.
- **Fix**: `skip-dirs: .venv`; scan `uv.lock` (0 CRITICAL).
- **Lesson**: scope a scanner to what you ship, then keep it strict.

## Milestone 8 — Kubernetes (kind)

### The Ingress was rejected by a webhook that wasn't up yet
- **What happened**: `kubectl apply -k` failed: `failed calling webhook
  "validate.nginx.ingress.kubernetes.io" … connection refused`.
- **Root cause**: ingress-nginx registers an admission webhook before
  its controller pod is ready.
- **Fix**: wait for `deployment/ingress-nginx-controller` before
  applying manifests.
- **Lesson**: "apply everything" has ordering constraints when
  admission webhooks are involved.

### A frozen Postgres left the ingestor "healthy" for five hours
- **What happened**: the liveness drill (`docker compose pause
  postgres`) produced no restart; the pod logged nothing from 20:27 to
  01:32 and `/health` stayed 200.
- **Root cause**: SIGSTOP freezes the server but keeps TCP alive; the
  DB call blocks forever, so the retry/exhaustion path never runs. The
  Milestone 3 liveness checked "thread alive", which was true.
- **Fix**: `last_progress_at` + `stall_timeout_seconds` in both loop
  services' liveness; proven with 3 kubelet restarts during a 6-minute
  pause.
- **Lesson**: liveness must measure progress. Death is only one way to
  stop working.

### 100% tracing of Kafka messages OOM-looped Tempo (113 restarts)
- **What happened**: Tempo sat at 509/512 MiB and restarted for hours;
  the tracing smoke timed out.
- **Root cause**: one span per message from the simulator and ingestor
  (~200+/s) after Milestone 6, unnoticed until the cluster work reran
  the smoke.
- **Fix**: 2% head sampling for those two services; Tempo 768 MB.
- **Lesson**: instrumenting a hot loop needs a sampling decision on
  day one, and a restart count belongs on the dashboard.

### Prometheus lost three targets when services moved into the cluster
- **Fix**: scrape both the Compose name and the kind NodePort per job;
  the smoke requires one up per job.
- **Lesson**: static scrape configs encode topology; every topology
  change must revisit them (or move to service discovery).

## Milestone 9 — Kubeflow Pipelines v2

### `@dsl.component` could not read the type hints
- **What happened**: `TypeError: Artifacts must have both a schema_title
  and a schema_version … Got: Output[Dataset]`.
- **Root cause**: `from __future__ import annotations` turns hints into
  strings; KFP needs the objects.
- **Fix**: no postponed annotations in `pipeline.py` (comment explains).

### The image's ENTRYPOINT ate the executor command
- **What happened**: every task died with `amel-train: error: argument
  command: invalid choice: 'sh'`.
- **Root cause**: `ENTRYPOINT ["amel-train"]` prepends to whatever KFP
  passes (`sh -c … executor_main`).
- **Fix**: `CMD ["amel-train", "train"]` instead; callers pass full
  commands.

### `ImagePullBackOff` for a locally built image
- **Root cause**: `:latest` implies `imagePullPolicy: Always`; the pod
  asked Docker Hub for `docker.io/library/amel-training:latest`.
- **Fix**: tag `amel-training:local` everywhere.

### The task pod had no `DATABASE_URL`
- **Root cause**: Compose injected it; nothing in the pipeline did.
- **Fix**: `kubernetes.use_secret_as_env(...)` from the `amel-secrets`
  Secret (created in the `kubeflow` namespace by `kfp_up.sh`); the
  Docker runner injects the same variable itself.

### A stale root-owned pipeline root blocked every container
- **Root cause**: the very first (entrypoint-broken) run created
  `/tmp/amel-kfp-local` as root; later tasks ran as uid 1000.
- **Fix**: user-owned root under `~/.cache`, containers run as the
  invoking uid.

### KFP 2.17 has no `minio` Deployment
- **Fix**: wait for `seaweedfs` instead — the artifact store changed.
