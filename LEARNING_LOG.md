# Learning Log

This log exists so the developer builds real understanding, not just a
working repo. After every major subsystem: what was built, why it exists,
how data/control flows through it, the important files, its failure
modes, how to test it, the concepts it demonstrates, and interview
questions it should let you answer confidently.

---

## Milestone 0 — Repository skeleton, tooling, GitHub remote

**WHAT WAS BUILT.** Project tooling and documentation conventions, no
runtime code: `pyproject.toml` (ruff + mypy + pytest configuration, `uv`
as the environment/dependency manager), `.python-version` pinning Python
3.12, a `Makefile` with working `install`/`lint`/`fmt`/`typecheck`/`test`
targets and placeholder infra targets (`up`/`down`/`logs`/`migrate`/
`seed`/`smoke`) that will become real in Milestone 1, `.gitignore`,
`.env.example`, and the required documentation set (`README.md`,
`ARCHITECTURE.md`, `PROJECT_STATE.md`, `DECISIONS.md`, `SECURITY.md`,
`RUNBOOKS.md`, this file). A GitHub remote was created and the initial
commit pushed and tagged `milestone-0`.

**WHY IT EXISTS.** Every later milestone depends on a reproducible,
documented starting point. Concretely: without a pinned Python version,
`pip install` behavior silently depends on whatever `python3` happens to
resolve to on a given machine — which is exactly the class of bug (a
dependency that resolves differently in dev vs. CI vs. prod) that causes
real production incidents. Without `PROJECT_STATE.md`/`DECISIONS.md`
maintained from the start, a project of this scope becomes unresumable
after a context reset — which defeats the stated purpose of this whole
repository (a future Claude Code session, or the developer alone, must be
able to pick this up cold).

**HOW DATA/CONTROL FLOWS THROUGH IT.** N/A at this milestone — no runtime
components exist. The "flow" here is developer workflow: `uv sync` reads
`.python-version` + `pyproject.toml` → creates `.venv` bound to a
uv-managed standalone CPython 3.12 interpreter (not the system's) →
`make lint`/`typecheck`/`test` all run through `uv run`, so they execute
inside that pinned environment regardless of what `python3` resolves to
on `$PATH`.

**IMPORTANT FILES.**
- `pyproject.toml` — tool configuration and the `[tool.uv] package =
  false` declaration (this is a dependency-management project, not an
  installable package, at least until a first real package is added).
- `.python-version` — read automatically by `uv`; this is the actual
  mechanism that makes the Python-version pin effective, not just
  documentation.
- `Makefile` — the single entry point for common operations; infra
  targets are intentionally `echo`-only stubs (exit 0) rather than
  missing targets, so `make smoke` etc. never breaks a clean checkout
  even before Milestone 1 exists.
- `DECISIONS.md` ADR-0001 — records *why* 3.12 was chosen over the
  system's 3.14, and why `uv` was chosen over `pyenv` specifically
  because this environment has no passwordless `sudo` and lacks the
  `libssl-dev`/`libsqlite3-dev`/etc. build toolchain `pyenv` needs to
  compile CPython from source.

**FAILURE MODES.** None yet at the systems level. The one real failure
avoided here: had this project used the system Python 3.14 without
checking, dependency installs for Airflow/Feast/Kubeflow SDK/Pandera/
MLflow in a later milestone could have failed with confusing "no matching
distribution" errors with no wheel for `cp314`, at a point in the project
where the cause would be much less obvious than it is right now.

**HOW TO TEST IT.** `make install && make lint && make typecheck && make
test` — all four should exit 0 on a clean checkout with only `uv`
pre-installed (no system Python 3.12, no root access required).
`tests/test_environment.py` specifically asserts the interpreter running
the test suite is 3.12 and that `ssl`/`sqlite3`/`bz2`/`lzma` all import
correctly (a standalone/minimal Python build can be missing one of
these).

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- Why pinning a language runtime version is a reliability practice, not
  bureaucracy — "works on my machine" is frequently a runtime-version
  problem.
- The difference between a system package manager (`apt`, needs root),
  a build-from-source version manager (`pyenv`, needs a toolchain), and a
  standalone-binary version manager (`uv`, needs neither) — and when each
  is the right tool.
- Why `uv.lock` (once dependencies exist) matters: a lockfile pins exact
  transitive dependency versions so "it worked in dev" reliably means "it
  will work in CI and prod," not just "it worked with whatever was
  latest on the day I ran `pip install`."
- Why documentation-as-code (`PROJECT_STATE.md` updated every milestone,
  not written once at the end) is itself an engineering practice: it's
  the same problem runbooks and ADRs solve — making tribal knowledge
  survive a change in who (or what) is operating the system.

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "How do you handle Python version management across dev/CI/prod, and
  why does it matter?"
- "A new CPython release breaks a dependency's wheel availability — walk
  me through how you'd diagnose and fix that in an existing project."
- "What's the tradeoff between a monorepo and per-service repos for a
  platform with this many moving parts?" (see `DECISIONS.md` ADR-0002)
- "How do you keep a long-running, multi-session project resumable when
  the people/agents working on it change over time?"

---

## Milestone 1 — PostgreSQL, Kafka, source simulator, stream ingestor

**WHAT WAS BUILT.** A uv workspace of four packages: `libs/amel_common`
(versioned Pydantic event schemas, structured logging), `libs/amel_db`
(SQLAlchemy models + Alembic migrations for the `landing` schema),
`services/source_simulator` (streams the UCI HIGGS dataset into Kafka as
realistic, imperfect traffic), and `services/stream_ingestor` (a Kafka
consumer group that idempotently persists that traffic into Postgres).
Docker Compose runs Postgres, Kafka (KRaft, no ZooKeeper), a one-shot
topic-creation container, a one-shot migration container, and both
services. `scripts/smoke_milestone1.py` drives the acceptance check.

**WHY IT EXISTS.** This is the data plane every later milestone builds
on: Milestone 2's Airflow DAG reads from `landing`, Milestone 3's Feast
feature store reads from `curated` (derived from `landing`), and so on.
Getting the delivery semantics right here — at-least-once Kafka delivery
made safe by an idempotent sink — is the foundational reliability
property of the whole platform; every downstream consumer of this data
inherits it.

**HOW DATA/CONTROL FLOWS THROUGH IT.**
1. `source_simulator` downloads `higgs.zip` once (streamed to disk,
   never loaded fully into memory — it's a ~2.8GB archive), then streams
   rows out of the zip's single CSV member (also never fully materialized
   — `zipfile.ZipFile.open()` + `csv.reader` read it lazily row by row).
2. Each row becomes a `FeatureEvent` (28 named HIGGS features) published
   to `higgs.features.v1`, keyed by a deterministic `entity_id`.
   Independently, a `LabelEvent` is generated for the same row and handed
   to a heap-based `DelayedDispatcher` that publishes it to
   `higgs.labels.v1` after `LABEL_DELAY_SECONDS` — modeling a label that
   genuinely arrives after its features, not just a fixed replay offset.
3. Before either publish, `random.Random(RANDOM_SEED)` decides (a) does
   this message get corrupted first (`malformed.py`, bypasses the
   Pydantic model entirely — the point is to produce a payload that
   *doesn't* validate) and (b) is a second, identical copy sent right
   after (simulating redelivery/producer-retry duplication upstream).
4. `stream_ingestor` consumes both topics in one consumer group, batches
   messages (`BATCH_SIZE` or `BATCH_TIMEOUT_SECONDS`, whichever comes
   first), validates each against the same Pydantic schema, and either:
   routes it to the matching `.dlq` topic (validation failure) or adds it
   to a bulk-insert batch (valid).
5. The batch is written in **one transaction**: `INSERT ... ON CONFLICT
   (event_id) DO NOTHING ... RETURNING event_id` — comparing rows
   returned to rows attempted tells us exactly how many were genuine
   duplicates, which becomes a metric, not a guess.
6. **Only after that transaction commits** does the consumer call
   `consumer.commit(offsets=..., asynchronous=False)` for exactly the
   offsets in that batch.

**The at-least-once + idempotent-sink argument, step by step** (this is
the crash scenario `AMEL_KICKOFF_PROMPT.md` asks to document precisely):
1. Kafka delivers a batch of messages to the consumer (not yet committed).
2. The consumer opens a Postgres transaction and inserts the batch with
   `ON CONFLICT DO NOTHING`.
3. The transaction commits — rows are now durably in `landing`.
4. **The process crashes right here**, before step 5.
5. (didn't happen) Kafka offset commit.
6. On restart, the consumer rejoins the group at the *last committed*
   offset — which is still before the batch from step 2-3, because step
   5 never ran.
7. Kafka redelivers that same batch.
8. The idempotent sink (`event_id` primary key + `ON CONFLICT DO
   NOTHING`) makes step 2's re-run a no-op for every row that made it
   into step 3's commit: `RETURNING` comes back empty for those rows,
   `duplicate_skipped` increments, and no duplicate row is written.

This is *why* offset-commit-after-DB-commit is the one ordering that
matters here: commit the offset first (before or without a guaranteed DB
write) and a crash between the two **loses data silently** — Kafka thinks
it's handled, but it never reached Postgres. Commit the DB write first
and make the sink idempotent, and the same crash costs nothing but a
harmless redelivery. This was verified directly, not just argued: see
the "stream_ingestor killed mid-batch" entry in `RUNBOOKS.md` — a real
`SIGKILL` mid-processing, restart, and a follow-up query proving
`count(*) == count(DISTINCT event_id)` afterward.

**Why DLQ offsets commit immediately (unlike a DB-write failure):** a
schema-invalid message will *never* become valid on redelivery — retrying
it is pure waste and, worse, a poison-pill message would block the
partition forever if never acknowledged. Routing it to the DLQ *is*
handling it, so its offset commits like any other successfully processed
message. Contrast this with a *DB* write failure (Postgres down, say):
that's transient, so the ingestor retries with exponential backoff and,
if retries are exhausted, deliberately **stops the process** rather than
either dropping the batch or committing offsets it didn't actually
persist — leaning on exactly the crash-recovery property above to make
the eventual restart safe.

**IMPORTANT CODE FILES.**
- `libs/amel_common/src/amel_common/schemas/events.py` — the schema
  contract every producer/consumer shares; `extra="forbid"` and the
  28-feature-exact-match validator are what make "malformed" mean
  something precise and testable.
- `services/stream_ingestor/src/stream_ingestor/consumer.py` — the whole
  delivery/consistency argument above is implemented in `_process_batch`
  / `_write_batch` / `_write_with_retry`; read it alongside this entry.
- `libs/amel_db/alembic/versions/0001_schemas_and_landing_tables.py` —
  provisions all six schema namespaces up front but only `landing`
  tables now; see `DECISIONS.md`-adjacent reasoning in the migration's
  own docstring.
- `services/source_simulator/src/source_simulator/dataset.py` — streams
  a multi-GB zip without ever fully materializing it in memory or on
  disk as an extracted CSV.

**FAILURE MODES (exercised, not hypothetical — see RUNBOOKS.md).**
- Stream ingestor killed mid-batch → safe redelivery, zero duplicate
  rows, verified by direct query.
- Malformed event (four corruption strategies) → DLQ, offset committed,
  raw bytes preserved (base64) for inspection.
- Kafka broker briefly unavailable to the simulator's producer → handled
  by `enable.idempotence=True` + `acks=all` + librdkafka's own retry
  logic; not yet deliberately chaos-tested against a killed broker (that
  exercise is still open — a good candidate for the Milestone 7+
  Failure Engineering pass).
- A real-world data-engineering lesson, not a code failure mode but worth
  recording: `dataset.py` was first written assuming the UCI zip contains
  a plain `HIGGS.csv`, based on the dataset's documentation page. The
  real archive actually contains `HIGGS.csv.gz` — the CSV is gzip'd
  *inside* the zip. This was only caught by testing against the actual
  downloaded file, not the hand-built test fixture (which matched the
  assumption, not reality) — a concrete instance of why "unit tests pass"
  and "verified against production data" are different claims, and why
  this milestone's Definition of Done requires the latter, not just the
  former. Fixed in `_find_csv_member`/`stream_rows` (handles both `.csv`
  and `.csv.gz` members now) with a regression test for the gzip case.

**HOW TO TEST IT.**
- Unit tests (`make test`, no infra required): dataset streaming/cycling
  against a synthetic zip fixture, malformed-payload corruption logic,
  the delayed-dispatcher's ordering guarantee, schema validation
  (accepts exactly 28 named features, rejects missing/extra/out-of-range
  values), and the ingestor's `_validate()` against hand-built JSON.
- Integration/smoke (`make up` then `make smoke`): waits for
  `SMOKE_MIN_FEATURE_EVENTS` (default 100,000) rows in
  `landing.higgs_feature_events`, asserts `count(*) ==
  count(DISTINCT event_id)`, and confirms at least one message landed in
  each DLQ topic.
- Manual chaos: `docker kill -s SIGKILL <stream-ingestor container>`,
  restart it, re-run the duplicate-count query — see RUNBOOKS.md for the
  exact commands and the actual numbers observed.

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- At-least-once vs. exactly-once vs. at-most-once delivery, and why
  "idempotent at-least-once" is usually the right target rather than
  chasing true exactly-once (which Kafka can approximate transactionally
  but at real complexity/latency cost this project doesn't need yet).
- Why offset-commit ordering relative to the side effect it protects is
  the entire ballgame for delivery guarantees — this generalizes far
  beyond Kafka (any queue + any sink has this same commit-ordering
  question).
- Consumer groups, partition assignment, and rebalance timing (the
  observed ~28s reassignment delay after a hard kill is the session
  timeout doing its job, not a bug).
- Dead-letter queues as a *deliberate handling path* with their own
  commit semantics, not just "a topic we dump errors into."
- Why a uv workspace (shared lockfile, per-package pyproject.toml) beats
  either one monolithic dependency list or fully separate repos for a
  monorepo at this stage — see `DECISIONS.md` ADR-0002.

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "Walk me through exactly what happens if your consumer crashes between
  writing to the database and committing its Kafka offset. Why is that
  ordering safe?"
- "How do you make a batch database write idempotent without a separate
  dedup table?" (`ON CONFLICT DO NOTHING ... RETURNING`, and why
  `RETURNING` specifically lets you *measure* duplicates instead of just
  tolerating them silently.)
- "Why does a dead-letter queue message get its offset committed
  immediately, while a database outage doesn't?"
- "What's the difference between what `auto.offset.reset=earliest` does
  on a brand-new consumer group versus a consumer group with existing
  committed offsets? Why did that matter (or not) in your restart test?"
- "How would you decide `BATCH_SIZE` and `BATCH_TIMEOUT_SECONDS` for a
  real workload, and what's the tradeoff?" (larger/longer batches: fewer
  offset commits and better DB throughput, but a bigger
  guaranteed-redelivery window on crash.)

---

## Milestone 2 — Airflow + MinIO + bronze/silver/gold batch pipeline

**WHAT WAS BUILT.** A single Airflow 3.3.2 container (`LocalExecutor`,
`airflow standalone`) running `higgs_pipeline`, a 10-task DAG:
`determine_high_watermark → extract_new_records → write_bronze →
validate → transform → write_silver → update_curated_tables →
build_training_dataset → update_feature_store (stub) →
emit_pipeline_metadata`. MinIO provides the `bronze`/`silver`/`gold`/
`pipeline`/`mlflow` buckets. New Postgres tables:
`curated.higgs_features`, `curated.higgs_labels`,
`curated.training_records`, `control.pipeline_watermarks`,
`control.pipeline_runs`. Business logic lives in
`infra/airflow/dags/higgs_pipeline_tasks.py` (no `airflow.*` imports —
unit-tested in the ordinary uv workspace venv); `higgs_pipeline.py` is
the thin Airflow wrapper. A new `libs/amel_lake` workspace package holds
the MinIO client, deterministic object-key builders, and a Pandera-based
validation layer.

**WHY IT EXISTS.** `landing` is an operational write path (Milestone 1's
job: get Kafka data into Postgres safely). Nothing downstream should
query it directly forever — it has no data-quality gate, no typed/flat
feature columns, no durable object-storage copy, and no
point-in-time-correct training view. This milestone builds the layer
that turns "safely landed" into "trustworthy and queryable at scale":
bronze (raw durable copy), silver (validated, typed, analytics-ready),
curated (deduplicated Postgres tables other services can query directly,
Milestone 3's Feast feature store included), gold (training-ready
joined dataset).

**HOW DATA/CONTROL FLOWS THROUGH IT.**
1. `determine_high_watermark` reads `control.pipeline_watermarks` for
   two independently-tracked streams (`higgs_features`, `higgs_labels`)
   and computes `upper_bound` — `data_interval_end` for a scheduled run,
   or `dag_run.run_after` for a manual trigger (which has no data
   interval at all — this was the first bug found; see below).
2. `extract_new_records` queries `landing.*` for
   `ingested_at > watermark AND <= upper_bound`, writing raw results to
   local staging (`/opt/airflow/staging/<run_id>/`, a Docker volume) —
   inter-task hand-off, not the durable copy.
3. `write_bronze` uploads that same data to MinIO, keyed by `run_id`
   (idempotent — retrying a run overwrites the same object).
4. `validate` runs it through Pandera schemas (28 HIGGS features
   bounds-checked, duplicate-`event_id` detection, timestamp sanity,
   target ∈ {0,1}), quarantines invalid rows, writes a JSON report to
   MinIO, and fails the task if `invalid_fraction` exceeds a threshold.
5. `transform` flattens the nested `features` JSON into 28 typed float
   columns — bronze keeps the raw nested shape; silver is analytics-ready.
6. `write_silver` uploads the transformed data to MinIO.
7. `update_curated_tables` upserts into `curated.higgs_features`/
   `higgs_labels` (`ON CONFLICT DO NOTHING`, same pattern as Milestone
   1's sink) and advances both watermarks — **in one transaction**, so a
   failure here never leaves the watermark ahead of what's durably
   curated.
8. `build_training_dataset` joins the *accumulated* curated tables (not
   this run's delta) for entities missing from
   `curated.training_records`, upserts the new rows, and writes a gold
   Parquet snapshot of just the newly-completed joins.
9. `update_feature_store` is a deliberate no-op — Feast lands in
   Milestone 3.
10. `emit_pipeline_metadata` writes one `control.pipeline_runs` row
    summarizing the whole run (every count, every object key). A DAG-level
    `on_failure_callback` writes a `status='failed'` row from whichever
    task actually raised, so a failed run is never silently invisible.

**Why the join in step 8 is against curated state, not the run's
delta.** `LABEL_DELAY_SECONDS` means a feature event can be ingested
before its label. If `build_training_dataset` only looked at *this run's*
newly-extracted features/labels, a feature whose label arrives in a
*later* run would never get joined — its feature row was already past
the watermark by the time the label showed up. Joining against the full,
ever-growing `curated.higgs_features`/`higgs_labels` (filtered to
entities not yet in `training_records`) means a late label is picked up
correctly the first run after it lands, regardless of when its feature
was curated. This is the load-bearing reason `update_curated_tables`
(accumulate) and `build_training_dataset` (join-against-accumulated) are
separate steps rather than one.

**THREE REAL BUGS FOUND AND FIXED DURING THE ACCEPTANCE RUN** (not
hypothetical — each was caught by actually running the DAG against a
live, growing dataset, which is exactly why "run it for real" is part of
this project's Definition of Done, not just "unit tests pass"):

1. **`KeyError: 'data_interval_end'` on manual triggers.** A
   schedule-driven run always has a `data_interval_end`; a manually
   triggered run (`airflow dags trigger`, no explicit logical date) has
   *no data interval at all* — the key is simply absent from the task
   context, not `None`. Fixed with a `_upper_bound(context)` helper that
   falls back to `dag_run.run_after`. Lesson: test the code path a human
   operator will actually use (`airflow dags trigger` for an ad-hoc
   run/backfill), not only the path the scheduler exercises.
2. **`psycopg.OperationalError: number of parameters must be between 0
   and 65535`.** Postgres hard-caps bind parameters per query. A single
   `INSERT ... VALUES (...), (...), ...` built from one row per Airflow
   task execution scales with backlog size — at ~48k extracted rows × 6
   columns that's already ~288k parameters, blowing the limit by 4x.
   Fixed by chunking every bulk upsert (`UPSERT_BATCH_SIZE = 2000`) in
   `higgs_pipeline_tasks.py`. Lesson: a bulk upsert that works fine in a
   small dev test can hit a hard platform limit the moment a real backlog
   exists — this is exactly why the acceptance run used the real,
   continuously-growing dataset rather than a fixed small fixture.
3. **Watermark could silently regress under concurrent retries.** Several
   DAG runs ended up `up_for_retry` simultaneously (an artifact of
   iterating on bug #1/#2 live against a running scheduler) and were all
   retried together once the image was rebuilt. `advance_watermark`'s
   original `ON CONFLICT DO UPDATE SET watermark = <new value>`
   unconditionally overwrites — whichever transaction *committed last*
   wins, regardless of which had the logically later `upper_bound`.
   Observed directly: the watermark ended up at `20:10:00` even though
   runs with `upper_bound` `20:13:19` and `20:15:00` had already
   succeeded, because a still-in-flight `20:10:00` run committed after
   them. Not data-lossy (a regressed watermark only causes redundant,
   safely-deduplicated re-extraction), but incorrect "high watermark"
   semantics. Fixed with `SET watermark = GREATEST(current, new)` in the
   upsert (`amel_lake/watermark.py`), verified by (a) a unit test that
   inspects the compiled SQL for `GREATEST`, and (b) triggering a fresh
   run after the fix and confirming the watermark advanced from the
   regressed `20:10:00` to `20:25:00` — and that the resulting wide
   re-extraction window (re-processing already-curated data) produced
   **zero duplicate rows** in any curated table, which is itself a live
   demonstration that the idempotent-upsert design tolerates exactly this
   kind of watermark misbehavior gracefully. A related, lower-stakes
   version of the same class of bug: the ORM model's
   `onupdate=func.now()` on `updated_at` never fires through a raw Core
   `INSERT ... ON CONFLICT DO UPDATE` (only through the ORM unit-of-work)
   — fixed by setting `updated_at` explicitly in the same `SET` clause.

**IMPORTANT CODE FILES.**
- `infra/airflow/dags/higgs_pipeline_tasks.py` — all business logic;
  read `update_curated_tables_and_advance_watermarks` and
  `build_training_dataset` together to see the atomicity/join argument
  above.
- `infra/airflow/dags/higgs_pipeline.py` — Airflow wrapper: `_upper_bound`
  (bug #1), the `on_failure_callback`, task wiring.
- `libs/amel_lake/src/amel_lake/watermark.py` — the `GREATEST`-based
  monotonic advance (bug #3).
- `libs/amel_lake/src/amel_lake/validation.py` — Pandera schemas and the
  quarantine-invalid-rows pattern.
- `DECISIONS.md` ADR-0004 — why Airflow gets its own image/database/
  `standalone` deployment instead of joining the uv workspace.

**FAILURE MODES (exercised, not hypothetical).**
- Manual trigger with no data interval → handled (bug #1 above).
- Large backlog blowing the Postgres parameter limit → handled via
  batched upserts (bug #2).
- Concurrent/out-of-order task retries racing on shared state (watermark,
  curated tables) → idempotent upserts made this safe by construction;
  the watermark's own monotonicity needed an explicit fix (bug #3) even
  though the *data* was never at risk.
- Empty extraction window (a scheduled run landed before any data
  existed) → `validate`/`transform`/`write_*` all handle a zero-row
  DataFrame without error (see `test_empty_dataframe_is_trivially_valid`
  and the real `scheduled__20:05:00` run, which processed 0 rows
  end-to-end successfully).

**HOW TO TEST IT.**
- Unit tests (`make test`, no infra required): `WatermarkBounds`,
  `transform_features`/`transform_labels` dtype coercion, `_chunked`,
  the `GREATEST`/`updated_at` SQL-construction checks, Pandera validation
  against synthetic DataFrames (valid, out-of-range, duplicate-ID,
  out-of-domain target).
- Integration (`make up`, then trigger the DAG): `airflow dags trigger
  higgs_pipeline`, `airflow tasks states-for-dag-run higgs_pipeline
  <run_id>` to watch it, then query `control.pipeline_runs` /
  `control.pipeline_watermarks` and `count(*) = count(DISTINCT ...)` on
  every curated table.
- Acceptance evidence from this session: 7 DAG runs, all `success`;
  final curated counts in the high hundreds of thousands per table, zero
  duplicates in any of them; MinIO holds bronze/silver/gold/validation-
  report objects for every run.

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- Watermark-based CDC extraction vs. fixed-window batch extraction, and
  why this pipeline uses the former (events arrive continuously with
  variable lag, not in neat scheduler-aligned windows).
- Medallion architecture (bronze/silver/gold) as a concrete pattern, not
  just a buzzword: what each layer is *for* and why silver's schema
  differs from bronze's (nested-raw vs. flat-typed).
- Why "join against accumulated state" is sometimes required instead of
  "join against this batch's delta" — the general problem of late-arriving
  related data, which recurs constantly in real data engineering
  (SCD dimension joins, delayed fact tables, etc.).
- Idempotency as a system property that has to be verified, not assumed:
  the watermark regression bug shows that even a system built with
  idempotent upserts everywhere can still have a *metadata* bug (the
  checkpoint itself) that only shows up under concurrency.
- Hard platform limits (Postgres's 65535 bind parameters) as a real
  operational constraint that scales with data volume, not something
  that shows up in a small local test.

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "Design a pipeline that turns continuously-arriving Kafka-sourced
  Postgres data into a training-ready dataset, handling the fact that
  labels arrive after features." (watermark-based extraction + curated
  accumulation + join-against-accumulated-state, not run-delta joins)
- "Your batch upsert works in dev but fails in production with a
  'too many parameters' error — what's happening and how do you fix it?"
- "How would a checkpoint/watermark value end up wrong even though every
  individual data write was correctly idempotent? How do you prevent
  that?" (unconditional overwrite vs. monotonic `GREATEST` advance under
  concurrent/out-of-order commits)
- "Why keep bronze and silver as separate zones instead of just writing
  directly to a clean, typed table?" (raw/reprocessable audit copy vs.
  consumption-ready shape; reprocessing silver from bronze doesn't need
  to re-hit the source system)
- "What's the tradeoff of joining against a run's own extracted delta
  versus the full accumulated curated state?" (delta: cheap, misses
  late-arriving related data; accumulated: correct, but the query grows
  with curated table size — this pipeline chose correctness and flagged
  the growing-scan cost as a known, documented limitation rather than
  solving incremental materialization here.)

## Milestone 3 — Feast + Redis feature store

**WHAT WAS BUILT.** A Feast 0.66 feature repository (`ml/feature_repo`:
one `entity_id` entity, one `higgs_features` feature view with the 28
HIGGS float fields, `tags={"version": "1"}`) whose offline source is
`curated.higgs_features_flat` — a Postgres VIEW (Alembic `0003`) that
flattens the JSONB `features` column into typed columns — and whose
online store is Redis 7. Feast's registry is a SQL registry in a new
`feast` database on the existing Postgres. Three one-shot Compose
services (`feast-apply`, `feast-materialize`, `feast-demo`) and one
always-on `feast-server` (`feast serve`, 1 GB cap). Chunked
materialization (`scripts/materialize.py`) replaces the Feast CLI.
The DAG's `update_feature_store` stub is now real: it finds the rows the
current run inserted (by `source_run_id`), splits them into ≤25k-row
windows, and `POST`s each to `feast-server /materialize`. Along the way
the simulator and ingestor got honest liveness/readiness probes, a
`HIGGS_START_INDEX`, and a failure-engineering script that reproduces
the producer wedge. ADR-0005 records the decisions.

**WHY IT EXISTS — what a feature store solves.**
- *Point-in-time correctness.* A training row is "entity E, at time T,
  had label Y" — the features must be the values that were true *at T*,
  not the latest ones. Feast's `get_historical_features(entity_df,
  features)` performs that as-of join for you: for each (entity,
  timestamp) in the entity dataframe it finds the newest feature row
  with `event_timestamp <= T` (within TTL). The demo proves both
  directions: label timestamps (10 s after the feature event) get all 28
  features; the *same entities as-of one day earlier* get nothing
  (0/20 rows with any feature) — a naive "latest value per entity" join
  would have leaked the future into training.
- *Train/serve skew prevention.* Training reads offline (Postgres),
  inference reads online (Redis), but both go through the **same
  feature definitions** — same names, same types, same source — so the
  number a model was trained on is the number it will be served.
  The demo asserts this directly: `lepton_pt` from Redis == `lepton_pt`
  from Postgres for the same entities (5/5), and after the DAG run the
  newest curated entity (`higgs-000938837`) returns identical values
  from `feast-server /get-online-features` and from the flat view.
- *Online/offline consistency as a managed process.* "Materialize" is
  the explicit, recorded act of copying the latest feature values into
  the online store. Feast records every materialized interval in the
  registry, so "what is in Redis" is a fact you can query, not a hope.
  An entity that landed *after* the last run is (correctly) absent
  online — the demo checks that too.
- *One catalogue.* Feature definitions live in code (`definitions.py`),
  are applied to a registry, and carry a version tag; a training run
  (Milestone 4) can record exactly which definition it used, and the
  registry's `feature_view_version_history` table shows every change.

**WHAT A FEATURE STORE DOES *NOT* SOLVE.**
- It does not make your data correct. Feast joins on whatever
  `event_timestamp` you give it; if timestamps are assigned at publish
  time (as this simulator does) rather than at the physical event, the
  as-of join is exactly as meaningful as those timestamps. It does not
  deduplicate, validate, or backfill — Milestones 1–2 do that upstream.
- It does not solve freshness. Redis is only as fresh as the last
  materialization; here that is "up to the last 5-minute DAG run". A
  real-time feature needs a streaming push path (Feast's `push`
  sources), which is a different architecture.
- It does not solve memory or scale by itself. Feast's Postgres
  offline store materialization loads a whole window into memory
  (~14 KB/row observed → 12 GB over 885k rows). The chunking is ours,
  not Feast's.
- It is not a model registry, not a metadata graph, not lineage. It
  knows "feature view v1 was materialized for [t0, t1)"; it does not
  know which model consumed it — that is MLflow's job (Milestone 4/5).
- It does not remove the need for the *same transformation code* on
  both paths when features are derived: our features are raw columns,
  so this milestone side-steps it; a derived feature (say, a rolling
  mean) would need an on-demand feature view or a shared transform
  library to keep both paths identical.

**HOW DATA/CONTROL FLOWS THROUGH IT.**
1. `feast-apply` (one-shot, after `migrate`) registers the entity, the
   Postgres source, and the feature view in the `feast` DB registry.
2. Backfill: `make feast-materialize` → `scripts/materialize.py` reads
   the registry's last materialized end (or the source's `min(
   event_timestamp)` on first run), computes every 25,000th
   `event_timestamp` in SQL as window boundaries, and calls
   `store.materialize(lo, hi)` per window. Each window: one SQL
   `SELECT ... WHERE event_timestamp BETWEEN lo AND hi` (latest row per
   entity), rows → protobufs → Redis `HSET` per entity.
3. Steady state: every DAG run's `update_feature_store` task selects
   `min/max(event_timestamp)` and the row-count boundaries for
   `curated.higgs_features WHERE source_run_id = <this run>`, then
   `POST http://feast-server:6566/materialize {start_ts, end_ts,
   feature_views:["higgs_features"]}` per window (synchronous; the task
   fails loudly on any HTTP error so Airflow retries). Zero inserted rows
   → no HTTP call, `nothing_to_materialize`.
4. Training path (Milestone 4 will use it): build an entity dataframe
   from `curated.higgs_labels` (`entity_id`, `label_timestamp` as
   `event_timestamp`, `target`) → `store.get_historical_features(...)`.
5. Inference path: `store.get_online_features(features, entity_rows)`
   from Python, or `POST feast-server /get-online-features` over HTTP —
   what `apps/inference_api` (Milestone 5) will call.

**THE INCIDENT, AND WHAT IT TAUGHT.** The first attempt at this
milestone froze WSL2 mid-materialize. On the retry it turned out to be a
chain of four distinct failures, each masked by the previous one:
1. `feast materialize` reached 12.1 GB RSS and was OOM-killed (the day
   before, the same thing froze the VM). Fix: chunking + container
   memory caps.
2. While Kafka was starved by (1), the simulator's producer timed out
   every message and its loop thread died — with `/health` returning
   200. Fix: delivery reports → `ProducerHealth` → `/ready` and
   `/health` derived from real state; the loop survives transient
   errors. Regression: `make failure-simulator-wedge`.
3. Kafka also evicted the ingestor from its consumer group; the next
   `commit()` raised and killed *its* loop — also with `/health` at
   200, ~870k lag, zero group members. Fix: a rejected commit is a
   redelivery, not a crash; loop death fails liveness.
4. Once (2) and (3) were fixed, restarting the simulator replayed
   885k already-landed ids. Fix: `HIGGS_START_INDEX`.
Only after all four did new data flow end-to-end into Redis. The lesson
that generalizes: **a health endpoint that cannot fail hides every
other failure**, and Milestone 8's Kubernetes probes would have
inherited the lie.

**IMPORTANT CODE FILES.**
- `ml/feature_repo/definitions.py` — the entity, source, feature view,
  version tag, and the TTL comment.
- `ml/feature_repo/feature_store.yaml` — registry/offline/online config
  and the `sslmode` note.
- `ml/feature_repo/scripts/materialize.py` — chunked backfill; read
  `_chunk_boundaries` and `plan` for the resume logic.
- `ml/feature_repo/scripts/demo_retrieval.py` — the acceptance demo:
  historical (positive + negative as-of), online, consistency.
- `infra/airflow/dags/higgs_pipeline_tasks.py` —
  `materialize_windows_for_run` / `update_feature_store`.
- `libs/amel_db/alembic/versions/0003_curated_higgs_features_flat_view.py`.
- `services/source_simulator/src/source_simulator/simulator.py` —
  `ProducerHealth`, `liveness()`, `readiness()`, `_on_publish_error`.
- `services/stream_ingestor/src/stream_ingestor/consumer.py` — the
  probe contract in the module docstring and the commit `except`.
- `scripts/failure_engineering/simulator_producer_wedge.py`.
- `DECISIONS.md` ADR-0005; `RUNBOOKS.md` (five new entries).

**FAILURE MODES (exercised, not hypothetical).**
- Unbounded materialization → OOM (12.1 GB) → fixed by chunking, peak
  875 MiB, containers capped.
- Broker paused for 30+ s → simulator `/ready` 503 in 32 s with the
  Kafka error, 294 delivery failures counted, self-recovery in 0 s
  after unpause, `/health` correctly stays 200 (transient, not fatal).
- Consumer-group eviction → commit rejected → loop continues, rejoins,
  redelivers; idempotent sink absorbs it.
- DAG run with zero new rows → `nothing_to_materialize`, no HTTP call.
- Entity requested online before it is curated → `None`, not a stale
  or fabricated value.

**HOW TO TEST IT.**
- Unit (`make test`, 69 tests, no infra): feature-view schema/version,
  window splitting (both the backfill script and the Airflow task), the
  task's one-POST-per-window/skip/propagate-error behaviour, both
  services' probe state machines, `HIGGS_START_INDEX` offsetting.
- Integration: `make up` → `make feast-materialize` → `make feast-demo`
  (prints and asserts the four properties above). Then trigger
  `higgs_pipeline` and read `update_feature_store_done` in the task log;
  `redis-cli DBSIZE` must equal `count(*)` of `curated.higgs_features`.
- Failure engineering: `make failure-simulator-wedge` (pauses Kafka for
  ~30 s; the stack keeps running).

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- As-of (point-in-time) joins and why "latest value" joins leak the
  future; TTL as the bound on how stale an as-of match may be.
- Offline vs online stores as *different access patterns over the same
  definitions* (columnar/bulk/historical vs key-value/latest/low
  latency), and materialization as the bridge.
- Train/serve skew: where it comes from (different code paths,
  different data, different timing) and which part a feature store
  removes (definition and data) versus not (derived-feature code).
- Liveness vs readiness: "restart me" vs "don't send me work"; why each
  must be computed from real state; why a rejected Kafka commit is a
  readiness event and a dead loop is a liveness event.
- Idempotent producers (librdkafka `enable.idempotence`), message
  timeouts, and what a fatal producer error means.
- Memory as a first-class constraint: batch jobs with memory
  proportional to input must be chunked, and containers must be capped
  so failures stay local.

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "What does a feature store give you that a well-indexed feature table
  doesn't?" (as-of joins, one definition for both paths, recorded
  materialization, versioned catalogue — and what it still leaves to
  you.)
- "How do you prove a training set has no label leakage from features?"
  (Request features as-of a time before they existed; expect nulls.)
- "Your Kubernetes pod is Running and Ready but doing no work. What
  went wrong with its probes, and how do you design probes that can't
  do that?"
- "A Kafka consumer's offset commit fails with `UNKNOWN_MEMBER_ID` —
  should it crash?" (No: with an idempotent sink it should rejoin and
  accept redelivery; it should *report* degraded readiness.)
- "A batch job OOMs on production data but not in dev. Walk me through
  bounding its memory without changing its output." (Chunk by row count
  computed from the data, resume from recorded progress, cap the
  container.)
- "Why put a feature server between the orchestrator and the online
  store instead of importing the SDK in the DAG?" (Dependency isolation,
  a real service boundary, memory containment, and it mirrors production
  topology.)

## Milestone 4 — Training package + MLflow

**WHAT WAS BUILT.** `ml/training` (`amel-training`): `config.py`
(every knob that changes an outcome, `TRAINING_*` env), `dataset.py`
(entity dataframe from `curated.higgs_labels` up to `as_of` →
`get_historical_features` through Feast's offline path), `split.py`
(seeded, stratified train/validation/test), `evaluate.py` (accuracy,
precision, recall, F1, ROC-AUC, log loss; confusion matrices; feature
importances; PNG plots), `provenance.py` (dataset fingerprint, git SHA),
`train.py` (one MLflow run: params, tags, metrics, artifacts, dataset
input, signature, registered model version aliased `candidate`),
`promote.py` (pure decision + audited apply), `cli.py`
(`amel-train train | promote | show`). An `mlflow` server container
(Postgres backend `mlflow` DB, MinIO `mlflow` bucket, proxied
artifacts) and a one-shot `train` container. Alembic `0004`
`ml.model_promotions`. ADR-0006.

**WHY IT EXISTS.** A model file is not a deliverable; a *reproducible
record* of how it was made is. Without tracking you cannot answer "which
data, which features, which code, which parameters produced the thing
in production, and how did it score?" — and without a registry with
aliases you end up hardcoding `v7` in the serving config. Without
promotion criteria and an audit trail, "champion" is whatever someone
last clicked.

**HOW DATA/CONTROL FLOWS THROUGH IT.**
1. `train`: read config → open Feast repo → read the feature view's
   `version` tag → query the newest `max_rows` labels with
   `label_timestamp <= as_of` → Feast point-in-time join (Postgres
   offline store) → drop labels with no feature match (383 of 200k here:
   their feature event was DLQ'd or not yet curated) → fingerprint the
   frame → stratified split (test carved first, then validation) →
   `mlflow.start_run` → log params/tags/`dataset_version.json`/
   `feature_definitions.json`/dataset input → fit → evaluate on
   validation *and* the untouched test split → log metrics, confusion
   matrices (JSON + PNG), importances (CSV + PNG), tree shape → log the
   model with an inferred signature and register it → alias
   `candidate` → tag the version with git SHA, fingerprint, feature
   view version, test metrics.
2. `promote`: resolve the candidate (or `--version`) → fetch its run's
   metrics → fetch the current champion's (if any) → `evaluate_promotion`
   → on approval (or `--force`) set the `champion` alias, tag the
   version (`promoted_by`, `promotion_reason`), tag the old champion
   (`superseded_by`), insert `ml.model_promotions`.
3. Serving (Milestone 5) loads `models:/higgs_decision_tree@champion`.

**WHAT "REPRODUCIBLE" MEANS HERE.** Same `as_of` + same config → same
dataset fingerprint → same split (seeded) → same tree (seeded) → the
same metrics to every decimal. Proven: v2 and v3 (`as_of
2026-09-19T21:00`) both fingerprint `ff72b440727cb949`, test accuracy
0.689787603526583, ROC-AUC 0.7585938300843371. Two runs *without*
`as_of` on a live table would legitimately differ — that is not
non-determinism, it is new data, and the logged `as_of` lets you pin
it afterwards.

**IMPORTANT CODE FILES.**
- `ml/training/src/amel_training/train.py` — read top to bottom; the
  order (log inputs *before* fitting) is deliberate.
- `ml/training/src/amel_training/promote.py` — `evaluate_promotion` is
  the whole policy; `promote` is the side effects.
- `ml/training/src/amel_training/provenance.py` — the fingerprint.
- `libs/amel_db/alembic/versions/0004_ml_model_promotions.py`.
- `infra/mlflow/Dockerfile`, the `mlflow`/`train` services in
  `infra/docker-compose.yml`.
- `DECISIONS.md` ADR-0006.

**FAILURE MODES (exercised).**
- Weak candidate (`max_depth=2`, test accuracy 0.6298) → promotion
  rejected with three reasons, exit 2, champion unchanged.
- MLflow at its memory cap on boot → topology trimmed (see MISTAKES.md).
- MLflow 3 Host-header rejection → `--allowed-hosts`.
- skops refusing the tree type → explicit trusted type.
- Blank Compose env → `None` at the config boundary.

**HOW TO TEST IT.**
- Unit (`make test`): split determinism/stratification/partitioning,
  fingerprint content-not-order sensitivity, metrics/confusion on a
  perfect predictor, importance ordering, every promotion branch,
  config parsing.
- Integration: `make up` → `TRAINING_AS_OF=<iso> make train` twice →
  compare `dataset_fingerprint` and metrics → `DECIDED_BY=<you> make
  promote` → `make model-show` → `SELECT * FROM ml.model_promotions`.
  MLflow UI at http://localhost:5000.

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- Experiment tracking vs model registry vs artifact store — three
  different things MLflow bundles.
- Aliases over stages/version numbers: why `@champion` is the only
  reference serving should hold.
- Dataset versioning by content fingerprint; why "the query" is not a
  version.
- Test split hygiene: carve it first, never tune on it.
- Model signatures and why schema enforcement at inference is a
  feature, not friction.
- Promotion as policy + audit: separate the *decision function* from
  the *side effects*, log both.

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "How do you make a training run reproducible when the training table
  is append-only and live?" (pin selection with `as_of`, fingerprint
  content, seed everything, log all of it)
- "What is the difference between an MLflow run, a logged model, a
  registered model version and an alias?"
- "Design an auditable model promotion process." (criteria as config,
  pure decision, alias flip + immutable audit row, forced overrides
  recorded, serving reads the alias)
- "Why would you refuse to pickle a model?" (skops / trusted types)
- "Your tracking server is at its memory limit doing nothing — what do
  you look at?" (worker count, side-car processes, caps)

## Milestone 5 — FastAPI inference

**WHAT WAS BUILT.** `apps/inference_api` (`inference-api`, FastAPI,
port 8003): `GET /health`, `GET /ready`, `GET /model`,
`POST /model/refresh` (admin token), `POST /predict/raw` (exactly the 28
HIGGS features, validated), `POST /predict/entity/{entity_id}`
(features from `feast-server`), `GET /metrics`. Modules: `model.py`
(`ModelCache` + `mlflow_loader`), `features.py` (`FeastOnlineClient`),
`store.py` (`ml.predictions`, Alembic `0005`), `publisher.py`
(`predictions.v1`), `service.py` (the request-independent prediction
path), `api.py`. `PredictionEvent` added to `amel_common.schemas`.
ADR-0007.

**WHY IT EXISTS.** The model is only useful if something serves it, and
serving is where every earlier guarantee is cashed in: the same feature
definitions (Feast), the champion alias (MLflow registry), honest probes
(Milestone 3), a persisted record and an event per prediction so the
platform (Milestone 11+) and FinOps (Milestone 16) can see what was
served, by which version, when.

**HOW A REQUEST FLOWS.**
1. `X-Trace-Id` is honoured or minted (propagation groundwork for
   Milestone 6's OpenTelemetry).
2. `/predict/entity/{id}`: `POST feast-server /get-online-features` →
   28 values or 404 if the entity has never been materialized.
   `/predict/raw`: pydantic rejects anything but exactly the 28 names.
3. A one-row DataFrame in `HIGGS_FEATURE_NAMES` order (the signature's
   order) → `predict_proba` → label at 0.5.
4. `ml.predictions` row (features, prediction, probability, model
   name/version/run, latency, trace id) → `PredictionEvent` to
   `predictions.v1` keyed by entity id → response with `prediction_id`,
   model metadata (version, run id, git SHA, dataset fingerprint,
   feature view), latency, trace id, `persisted`.

**HOW THE MODEL GETS THERE AND CHANGES.** Start-up: resolve
`@champion` → version → `mlflow.sklearn.load_model` (skops, trusted
type from the MLmodel config) → cache. Change: an operator promotes in
MLflow (Milestone 4), then calls `POST /model/refresh` — the API logs
`model_swapped previous=3 current=5` and every response's `model.version`
changes; nothing restarts. Proven live, including the negative cases
(401 without the token, no-op when the alias has not moved, failure
leaves the old model serving).

**IMPORTANT CODE FILES.**
- `apps/inference_api/src/inference_api/model.py` — read `refresh()`.
- `apps/inference_api/src/inference_api/service.py` — the whole
  prediction path without HTTP.
- `apps/inference_api/src/inference_api/api.py` — `Probes.readiness`
  and the `trace_id` note about `from __future__ import annotations`.
- `apps/inference_api/tests/test_inference_api.py` — the fakes show
  the seams.
- `DECISIONS.md` ADR-0007.

**FAILURE MODES (exercised).**
- MLflow stopped: predictions 200, `/ready` 200, refresh fails in 20 s
  with the previous model still serving, error clears on recovery.
- Unknown entity → 404 with reason; malformed raw payload → 422 naming
  the missing fields.
- Persistence failure (unit): response 200 with `persisted: false`,
  counter incremented, event still published.
- feast-server / Postgres / Kafka delivery degraded (unit): `/ready`
  503 with the specific reason; `/health` stays 200.
- No model at start-up (unit): live, not ready, 503 on predict, ready
  after a successful refresh.

**HOW TO TEST IT.**
- Unit (`make test`): the full request path against a real tiny tree
  with fake loader/features/store/publisher — 9 tests.
- Live: `make up`; `curl localhost:8003/model`; `curl -X POST
  localhost:8003/predict/entity/<id>`; `SELECT * FROM ml.predictions`;
  `kafka-console-consumer --topic predictions.v1`; promote a new
  champion and `POST /model/refresh` with `X-Admin-Token`.

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- Why model loading is a start-up/refresh concern and never a request
  concern; atomic swap under a lock.
- Readiness dependencies = what *this request* needs; MLflow is not one.
- Persist-then-publish and "the row is the record, the event is the
  notification".
- Train/serve skew guarded by a single source of column order.
- Trace-id propagation before tracing exists.

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "How do you roll a new model into a running API without a restart,
  and how do you roll back?" (alias → controlled refresh; re-alias +
  refresh)
- "Your model registry is down. What breaks?" (nothing at request
  time if the model is cached; refresh fails fast and reports)
- "Where do you validate inference inputs and why exactly there?"
- "What do you persist per prediction and what do you publish?"
- "Why does readiness not check the registry?"

## Milestone 6 — OpenTelemetry and observability stack

**WHAT WAS BUILT.** `amel_common.telemetry` (tracer + logger providers,
OTLP/HTTP export, FastAPI/SQLAlchemy/httpx/confluent-kafka
instrumentors, trace-id helpers) and a reworked `amel_common.logging`
that routes structlog through stdlib so an OTLP handler ships every
line with trace/span ids. All three Python services and the DB engine
call it; the inference API's `X-Trace-Id` is now the OTel trace id;
feast-server runs under `opentelemetry-instrument`; Airflow's native
OTel is on. Infra: OTel Collector (traces → Tempo, logs → Loki,
spanmetrics → Prometheus), Tempo 3, Loki 3, Prometheus 3, kafka-exporter,
Grafana 13 with provisioned datasources (Prometheus, Tempo, Loki, AMEL
Postgres), trace↔log links and an "AMEL overview" dashboard (11 panels).
`scripts/smoke_milestone6.py` / `make smoke-tracing`. ADR-0008.

**WHY IT EXISTS.** Milestone 3's incident was an hour of "healthy"
services doing nothing. Probes fixed the lie; observability answers the
next questions: *what* is slow, *where* a request spent its time, *which*
log lines belong to *this* request, and how the system looks over time.
Milestone 12's agents will read the same signals.

**HOW A PREDICTION IS TRACED.**
1. A caller sends `POST /predict/entity/{id}` (optionally with W3C
   `traceparent`; otherwise the FastAPI instrumentation starts a trace).
2. `inference_api` server span → httpx client span carrying
   `traceparent` → `feast_server` server span (auto-instrumented) →
   Redis `HMGET` client span → back → SQLAlchemy `INSERT amel` span →
   Kafka `predictions.v1 send` producer span (traceparent injected into
   message headers) → response with `X-Trace-Id` = the trace id.
3. The API's `prediction_request` log line carries the same
   `trace_id`/`span_id` → collector → Loki; Grafana's Tempo datasource
   links "logs for this span" to Loki and Loki's derived field links
   `trace_id` back to Tempo.
4. The collector's `spanmetrics` connector turns every span into
   `traces_span_metrics_calls_total` / `_duration_*` by service and
   route → Prometheus → the dashboard's RED panel.
Proven by `make smoke-tracing`: 13 spans, 2 services, 1 Loki line, 1
Postgres row, 6 Prometheus targets up, RED metrics for 5 services.

**HOW INGESTION IS TRACED.** The simulator's producer is wrapped, so
each `higgs.features.v1 send` is a producer span with `traceparent` in
the headers. The ingestor's consumer is wrapped: each `poll()` returns
a message and opens a `recv` span *linked* to the producer's span (not
parented — one poll can carry many producers' traces), and
`_process_batch` wraps the DB transaction in an `ingest_batch` span.

**IMPORTANT CODE FILES.**
- `libs/amel_common/src/amel_common/telemetry.py` — read top to bottom.
- `libs/amel_common/src/amel_common/logging.py` — `_add_trace_ids`
  and the stdlib routing.
- `apps/inference_api/src/inference_api/api.py` — `trace_id()`.
- `infra/observability/otel-collector/config.yaml` — the pipelines.
- `infra/observability/grafana/provisioning/datasources/datasources.yaml`
  — the trace↔log correlation config.
- `infra/observability/grafana/dashboards/amel-overview.json`.
- `scripts/smoke_milestone6.py`.

**FAILURE MODES / SURPRISES (exercised).**
- Tempo 3 rejects `compactor:`/`block_retention` YAML — retention is a
  `backend-scheduler` CLI flag now.
- Airflow's exporter selection ignores `OTEL_EXPORTER_OTLP_PROTOCOL`
  and speaks gRPC: pointing it at 4318 produced "Expected SETTINGS
  frame" errors until moved to 4317.
- The API minted its own `X-Trace-Id` before this milestone, so logs
  and traces had different ids — fixed by deriving it from the span.
- Kafka consumer spans arrive as separate linked traces; expecting one
  trace per message is a misunderstanding of OTel batch semantics.
- Grafana's Postgres datasource is `grafana-postgresql-datasource`
  (not `postgres`) in 13.x.

**HOW TO TEST IT.**
- Unit (`make test`): unchanged — telemetry is off by default.
- Live: `make up` → `make smoke-tracing`. Grafana http://localhost:3000
  (AMEL overview), Prometheus :9090, Tempo :3200, Loki :3100.
  `curl -s localhost:3200/api/search?q=...` / `loki/api/v1/query_range`.

**CONCEPTS THE DEVELOPER SHOULD UNDERSTAND.**
- Traces vs metrics vs logs, and the one id that joins them.
- W3C `traceparent` propagation over HTTP headers and Kafka message
  headers; spans, kinds (server/client/producer/consumer), links.
- Collector as the single fan-out point; why services never talk to
  Tempo/Loki directly.
- RED metrics derived from spans vs metrics a service emits itself.
- Sampling, retention and cardinality as the cost levers (none
  needed at this scale; all three would be the first knobs at real
  scale).

**INTERVIEW QUESTIONS THIS SHOULD LET YOU ANSWER.**
- "A user reports one slow prediction. Walk me from their response to
  the exact slow hop." (X-Trace-Id → Tempo → the 89 ms `feast_server`
  span → Redis or network)
- "How do logs get their trace id?" (processor reads the active span)
- "Why are your Kafka consumer spans not children of the producer
  span?"
- "What does an OTel Collector give you over exporting directly?"
- "What would you cap or sample first if this ran at 100× the volume?"
