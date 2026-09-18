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
