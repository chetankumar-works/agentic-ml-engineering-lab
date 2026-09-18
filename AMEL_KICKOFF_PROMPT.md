# Agentic ML Engineering Lab (AMEL) — Kickoff Prompt (v2, systematized)

Paste this as the first message to `claude` inside `~/projects/amel` in WSL2.
This revises the original spec: same technical scope, tighter operating
rules, an explicit Definition of Done, a recorded environment baseline, and
a GitHub workflow section, since this is a long, multi-session build.

---

## ROLE

You are the principal software architect and implementation engineer for a
learning-oriented but fully executable project named **Agentic ML
Engineering Lab (AMEL)**.

You are working with a data scientist who has working familiarity with
Python, FastAPI, ML modelling, Docker, Airflow, CI/CD and basic MLOps, but
is building this system specifically to develop deep practical
understanding of production ML engineering, distributed systems,
Kubernetes, observability and agentic engineering.

Your responsibility is not simply to generate code. You must construct an
executable system incrementally while maintaining documentation that
explains why the architecture works, and while keeping the repository in a
state any future Claude Code session (or the developer alone) can pick up
without the original conversation.

## VERIFIED LOCAL ENVIRONMENT (do not re-discover — validate against this)

This was confirmed working in WSL2 (Ubuntu 26.04 LTS) immediately before
this session started:

- Node.js: v22.23.2 (via nvm)
- Python: 3.14.4 (system) — **caution:** this is a very new release; some
  core dependencies in this spec (Airflow, Feast, Kubeflow SDK, Pandera,
  MLflow) may not yet publish wheels for 3.14. Before Milestone 1, check
  compatibility and, if needed, install Python 3.12 via `pyenv` or
  `deadsnakes` and pin the project to it with a `.python-version` file and
  a dedicated virtualenv. Do not silently fight version errors — resolve
  this explicitly and record the decision in `DECISIONS.md`.
- Docker: 29.7.2, Docker Compose: v5.4.0 (Docker Desktop, WSL2 integration
  enabled)
- git: 2.53.0
- Project directory: `~/projects/amel` (Linux filesystem inside WSL2 —
  deliberately not under `/mnt/c/...`, for I/O performance and to avoid
  permission issues with Docker bind mounts)
- Git repo already initialized locally, default branch renamed to `main`

Re-verify these at the start of Milestone 0 (a quick `node -v`,
`python3 --version`, `docker --version`, `docker compose version` is
enough) and record actual results in `PROJECT_STATE.md` rather than
assuming this document stays accurate forever.

## GIT & GITHUB WORKFLOW

- Remote: **`github.com/chetankumar-works/agentic-ml-engineering-lab`**,
  **public**. GitHub CLI (`gh`) auth is already configured in this WSL2
  environment — verify with `gh auth status` before relying on it; if it
  fails, stop and tell the developer rather than trying to embed
  credentials.
- If the repo does not yet exist on GitHub, create it with `gh repo create
  chetankumar-works/agentic-ml-engineering-lab --public --source=. --remote=origin`
  (run from `~/projects/amel`). If it already exists, just add it as
  `origin` and push.
- Commit at the end of every milestone, never mid-milestone with a broken
  state on `main`. Use Conventional Commit messages: `feat:`, `fix:`,
  `docs:`, `chore:`, `test:`, `ci:` — e.g. `feat(milestone-1): kafka stream
  ingestion with idempotent postgres sink`.
- After each milestone's commit, **push to `origin main`** and tag the
  commit `milestone-N` (e.g. `git tag milestone-1 && git push origin
  milestone-1`). This makes `PROJECT_STATE.md` and the tag history together
  enough for a fresh session to see exactly where the project stands.
- `.gitignore` must exclude: `.env`, `__pycache__/`, `.venv/`, `*.pyc`,
  Docker volumes data directories, MLflow/Airflow local state directories,
  downloaded HIGGS dataset files (large binary — document how to fetch it
  instead of committing it), and any credentials.
- Never commit secrets. `.env.example` documents required variables with
  placeholder values only.

## DEFINITION OF DONE (applies to every milestone, including Milestone 0)

A milestone is not complete until **all** of the following are true — file
creation alone never counts as done:

1. Code implements the milestone's actual requirements (no stub-only
   placeholders left where working logic was asked for).
2. Formatting/linting passes (`ruff` clean).
3. Type checking passes where configured (`mypy`, where practical).
4. Automated tests exist for the new surface area and pass (`pytest`).
5. The relevant services actually run (e.g. `docker compose up` for
   infra-dependent milestones) — not just "the code compiles."
6. A smoke test exercising the milestone's acceptance criteria has been
   run and its output captured/documented.
7. Any failures encountered were fixed, not hidden (no deleting a failing
   test to go green, no silently swallowing exceptions).
8. Working commands for this milestone are documented (README or the
   relevant service doc) exactly as run, not idealized versions.
9. `PROJECT_STATE.md` is updated: current milestone, completed work,
   current known failures/gaps, commands that work, next task.
10. `LEARNING_LOG.md` is updated per the Learning Requirement below.
11. A Conventional Commit is created for the milestone.
12. The commit is pushed to `origin main` and tagged `milestone-N`.

If a milestone cannot be fully completed in one sitting, stop at a clean
boundary, commit what's genuinely working (not partial broken state) as a
`wip:` commit if necessary, and leave `PROJECT_STATE.md` explicit about
exactly what remains — never claim a milestone is done when it isn't.

---

## PRIMARY OBJECTIVE

Build a fully working end-to-end ML engineering ecosystem demonstrating:

Python package and service architecture, type hints, async programming
where appropriate, pytest, FastAPI, REST, OpenAPI, authentication,
authorization, RBAC, Pydantic schemas, PostgreSQL, database schemas,
SQLAlchemy, Alembic, transactions, SQL, streaming ingestion, Kafka,
consumer groups, offsets, retries, dead-letter queues, idempotency, data
validation, lineage, MinIO/S3, Parquet, Airflow, Feast feature store,
Redis online feature serving, ML training, MLflow tracking, MLflow model
registry, Docker, Docker Compose, Kubernetes, Kubeflow Pipelines v2,
CI/CD, Git workflows, GitHub Actions, OpenTelemetry, logs, metrics,
distributed tracing, Prometheus, Grafana, Tempo, Loki, durable workflows,
Temporal, webhooks, MCP, LLM provider abstraction, agent planning, agent
tools, RAG/runbook retrieval, human approval, sandboxed actions, secrets,
failure recovery, horizontal scaling, event-driven scaling, SDK
development, CLI development, minimal web application, FinOps metadata,
serverless deployment abstraction.

The final repository must be executable and demonstrable. Avoid creating
non-functional placeholder architecture simply to satisfy the requested
component list.

## DATASET

Use the UCI HIGGS dataset. The complete source contains approximately 11
million examples. Do not require the entire dataset for normal
development. Support configuration such as `HIGGS_MAX_ROWS=500000` for
normal local development. Allow full-scale execution optionally. The
model is intentionally simple: `sklearn.tree.DecisionTreeClassifier`. This
project focuses on engineering rather than model sophistication.

## SOURCE SIMULATION

The original HIGGS dataset is static. Create a realistic event source
simulator. Implement `services/source_simulator`. It must:

1. download or locate the HIGGS dataset
2. stream rows rather than loading the entire dataset into RAM
3. assign: `event_id`, `entity_id`, `event_timestamp`, `schema_version`
4. publish feature events into Kafka topic `higgs.features.v1`
5. publish labels separately into `higgs.labels.v1`
6. support configurable artificial label delay
7. support configurable event rate
8. support burst mode
9. support deliberate duplicate generation
10. support deliberate malformed-event generation
11. support pause/resume
12. expose health and metrics endpoints

Environment variables should control behavior, e.g.:
`EVENTS_PER_SECOND=100`, `DUPLICATE_RATE=0.01`,
`INVALID_EVENT_RATE=0.002`, `LABEL_DELAY_SECONDS=10`. Use deterministic
random seeds where appropriate.

## EVENT SCHEMA

Define versioned Pydantic schemas. A feature event should contain
approximately: `event_id`, `entity_id`, `event_timestamp`,
`schema_version`, `source`, `features`, `trace_id`. A label event should
contain: `event_id`, `entity_id`, `target`, `label_timestamp`,
`schema_version`. Never pass arbitrary unvalidated dictionaries between
internal components when a typed schema is appropriate.

## KAFKA

Run Apache Kafka locally through Docker. Use modern Kafka configuration.
Create topics: `higgs.features.v1`, `higgs.labels.v1`,
`higgs.features.dlq`, `higgs.labels.dlq`, `predictions.v1`,
`platform.events.v1`, `audit.events.v1`.

Create a Python ingestion consumer. Do **not** initially use Kafka
Connect — the purpose is to learn consumer implementation. Implement
`services/stream_ingestor`. It must demonstrate: consumer groups, manual
or carefully controlled offset commits, batch consumption where
appropriate, database transactions, idempotency, retry, exponential
backoff, dead-letter handling, graceful shutdown, structured logs,
metrics, distributed tracing.

Use `event_id` uniqueness in PostgreSQL. Use `INSERT ... ON CONFLICT` or
equivalent protection. Document precisely what happens when:

1. Kafka delivery occurs
2. PostgreSQL transaction begins
3. database commit succeeds
4. process crashes before offset commit
5. Kafka re-delivers the message
6. idempotent handling prevents duplicate persistence

Explain why this constitutes at-least-once delivery combined with an
idempotent sink.

## POSTGRESQL

Use PostgreSQL. Create schemas: `control`, `landing`, `curated`, `ml`,
`audit`, `finops`. Use Alembic migrations. Suggested tables include:
`landing.higgs_feature_events`, `landing.higgs_label_events`,
`curated.higgs_features`, `curated.higgs_labels`,
`curated.training_records`, `ml.predictions`, `ml.model_metadata`,
`control.resources`, `control.resource_relationships`,
`control.platform_events`, `control.incidents`, `control.agent_runs`,
`control.agent_actions`, `audit.audit_events`, `finops.cost_events`,
`finops.provider_pricing`. Use constraints, indexes and foreign keys
deliberately. Document important index choices. Demonstrate real
transactions and rollback.

## AIRFLOW

Airflow must orchestrate batch/data lifecycle operations. Do not use
Airflow as the Kafka consumer. Create a DAG that runs at a configurable
interval:

`determine_high_watermark → extract_new_records → write_bronze → validate
→ transform → write_silver → update_curated_tables → build_training_dataset
→ update_feature_store → emit_pipeline_metadata`

Use proper task boundaries. Demonstrate: dependencies, retries, retry
delay, timeouts, XCom only for small metadata, task failure callbacks,
idempotent DAG execution. Store checkpoints/high-watermarks safely.

## MINIO DATA LAKE

Use MinIO as local S3-compatible object storage. Buckets/prefixes should
support: `bronze/higgs`, `silver/higgs`, `gold/training`,
`mlflow/artifacts`, `pipeline/artifacts`. Prefer Parquet for analytical
datasets. Partition by appropriate event date fields. Document the
distinction between Kafka event retention, PostgreSQL operational
relational storage, and MinIO durable object storage. Do not treat MinIO
merely as temporary scratch storage.

## DATA VALIDATION

Introduce a validation layer. Use either Pandera or an equivalent
lightweight Python validation framework. Validate: schema, types, null
expectations, target values, basic numeric bounds, duplicate IDs,
timestamp sanity. Invalid records must be observable. Produce validation
reports.

## FEATURE STORE

Use Feast. Build `ml/feature_repo`. Define entities and feature views.
Use PostgreSQL or generated Parquet data as the development offline
source. Use Redis as the online store. Demonstrate: historical feature
retrieval, point-in-time concepts, feature materialization, online
feature retrieval, feature versioning. Create a small working example
showing that training retrieves features through the offline path while
inference can retrieve latest features through the online path. Document
what feature stores solve and what they do not solve.

## TRAINING

Implement a reproducible training package using
`DecisionTreeClassifier`. Training must include: configuration,
train/validation/test split, random seed, model parameters, metrics,
confusion matrix, feature importances, model signature, dataset version,
Git commit SHA where available, feature definitions/version. No notebook
should be required for production training. A notebook may optionally
exist for exploration.

## KUBEFLOW PIPELINES

After local training works, implement Kubeflow Pipelines v2. Use
containerized components. Create components for: `load_training_dataset`,
`validate_training_dataset`, `split_dataset`, `train_model`,
`evaluate_model`, `register_model`. Compile pipeline definitions into
YAML. Do not introduce Kubeflow until the same training workflow works
outside Kubeflow. Document how KFP converts pipeline components into
Kubernetes workloads.

## MLFLOW

Run an MLflow tracking server. Use PostgreSQL as backend metadata store.
Use MinIO/S3-compatible storage for artifacts. Log: parameters, metrics,
artifacts, plots, model, dataset version, Git SHA, feature metadata,
training duration. Register successful models under
`higgs_decision_tree`. Use aliases such as `candidate` and `champion`. Do
not hardcode production model version numbers. Create validation criteria
before candidate → champion promotion. Champion promotion must be
explicit and auditable.

## INFERENCE API

Build `apps/inference_api` with FastAPI. Implement endpoints such as:
`GET /health`, `GET /ready`, `POST /predict/raw`,
`POST /predict/entity/{entity_id}`, `GET /model`, `GET /metrics`.
`/predict/raw` accepts validated features. `/predict/entity` retrieves
online features through Feast. Load the MLflow champion model. Cache
model safely and support controlled refresh. Persist prediction metadata.
Publish prediction events to Kafka. Return prediction ID and model
version.

## PLATFORM API

Build `apps/platform_api` — the primary control-plane API. Expose
resources such as: projects, events, incidents, models, pipelines,
agents, actions, costs, health. Use proper API versioning (`/api/v1/...`).
Generate OpenAPI documentation.

## SECURITY

Never hardcode credentials. Use `.env.example` for development
configuration only. Use secure secret injection. Implement development
authentication using signed JWTs. Support roles: viewer, developer,
operator, admin. Implement permission/scopes such as: `resources:read`,
`events:read`, `models:read`, `models:promote`, `agents:execute`,
`deployments:write`, `admin:all`. State-changing agent actions must
require authorization. Later provide Kubernetes Secret integration and
document migration toward Vault or a cloud secret manager. Never log
secrets.

## OBSERVABILITY

Instrument all first-party Python services with OpenTelemetry. Include:
trace IDs, structured logs, request duration, error counts, Kafka message
counts, consumer lag where available, database operation latency,
prediction latency, model identifier, agent execution duration. Deploy
OpenTelemetry Collector. Use Prometheus for metrics, Tempo for traces,
Loki for logs, Grafana for visualization. Correlate logs with trace IDs
wherever practical. Instrument FastAPI, database calls and Kafka flows
where possible.

## COMMON PLATFORM EVENT MODEL

Create a normalized platform event schema. Example sources: airflow,
mlflow, kubernetes, github, training, inference, agent, database, kafka.
Example event types: `airflow.task.failed`, `airflow.dag.completed`,
`model.registered`, `model.promoted`, `deployment.failed`,
`pod.oomkilled`, `consumer.lag.high`, `schema.validation.failed`,
`ci.failed`, `agent.action.proposed`. Store normalized events in
`control.platform_events` and optionally publish to
`platform.events.v1`.

## WEBHOOK SERVICE

Implement `apps/webhook_service`. Support at least a GitHub-style webhook
example. Webhook processing must: verify request authenticity, reject
invalid signatures, parse event, normalize event, persist event, publish
platform event. Document replay protection where appropriate. Provide
test fixtures for webhook events.

## DURABLE WORKFLOWS

Introduce Temporal after basic services work. Use Temporal for workflows
that may: wait, retry, survive restarts, require human approval,
coordinate multiple agent steps. Example workflow:

`incident detected → collect context → diagnose → propose remediation →
sandbox test → await human approval → execute authorized action →
validate → close incident`

Keep deterministic Temporal workflow code separate from non-deterministic
activities. Explain this distinction in documentation.

## AGENTIC CONTROL PLANE

Build a small but genuine agent system. Do not build an uncontrolled
autonomous agent. Implement: Agent Supervisor, Incident Agent, Airflow
Agent, ML Agent, Infrastructure Agent, Code/Repository Agent. The
supervisor receives: goal, user identity, available permissions, relevant
project context, resource metadata, recent events. It creates a
structured action plan using structured schemas rather than free-form
text, e.g.: goal, hypotheses, evidence_needed, planned_actions,
risk_level, required_permissions, approval_required.

## LLM PROVIDER ABSTRACTION

Do not couple the architecture permanently to Claude, OpenAI or any
single provider. Create an interface such as `LLMProvider` with methods
conceptually including `generate`, `generate_structured`, `embed`.
Implement adapters cleanly. Allow a mock provider so tests never require
paid APIs. Optional adapters may support Anthropic/OpenAI/local
providers.

## TOOL REGISTRY

Implement a tool registry. Each tool must declare metadata such as:
`tool_id`, `name`, `category`, `description`, `provider`, `free_or_paid`,
`risk_level`, `required_permissions`, `manual_guide`, `capabilities`.
Agent tools initially include read-only operations for: Airflow, MLflow,
PostgreSQL, Kubernetes, Git, observability. Separate read operations from
state-changing operations.

## AGENT SAFETY

Use risk classifications: `READ_ONLY`, `SANDBOX_WRITE`,
`REPOSITORY_WRITE`, `INFRASTRUCTURE_WRITE`, `DESTRUCTIVE`. Default
autonomous behavior should permit primarily `READ_ONLY` operations. State
changes must pass through policy evaluation. Potentially destructive
operations must require explicit human approval. Persist: requested
action, requesting agent, user, permissions, decision, execution result,
timestamp into audit storage.

## SANDBOX

Create an execution abstraction. For development, support a Docker-based
sandbox. The sandbox should: create isolated working directory/container,
apply proposed changes, run tests, capture output, destroy environment
when finished. Do not permit agent-generated commands to execute blindly
on the host. Document future migration to Kubernetes namespaces/jobs or
stronger isolation.

## MCP

Expose a subset of safe platform tools through an MCP server. MCP should
be an interface layer, not the location of business logic. MCP tools
should call existing application/service-layer functions. Start with
read-only capabilities. Examples: `list_airflow_dags`, `get_airflow_run`,
`list_mlflow_models`, `get_model_version`, `get_platform_events`,
`get_incident`, `query_resource_dependencies`. State-changing MCP
operations should be added only after policy controls exist.

## ENGINEERING METADATA GRAPH

Implement the first version using PostgreSQL rather than immediately
adding a graph database. Use `control.resources` and
`control.resource_relationships`. Model relationships such as: SERVICE
DEPENDS_ON DATABASE, DAG PRODUCES DATASET, MODEL TRAINED_FROM DATASET,
MODEL SERVED_BY SERVICE, SERVICE DEPLOYED_AS WORKLOAD, PIPELINE USES
FEATURE_VIEW. Provide an API to query upstream and downstream
relationships. Design interfaces so a graph database could replace or
complement this implementation later.

## RAG AND RUNBOOKS

Create a small internal documentation retrieval layer. Index:
`ARCHITECTURE.md`, `RUNBOOKS.md`, service READMEs, incident resolutions,
tool manuals. The Incident Agent should retrieve relevant runbook
material before making recommendations. Do not use RAG when deterministic
metadata queries answer the question directly.

## FINOPS

Create basic cost-accounting infrastructure. Track: LLM provider, model
name, input tokens, output tokens, estimated API cost, agent run, tool
execution, compute runtime where known, storage usage where easily
available. Persist normalized records into `finops.cost_events`. Use a
provider pricing abstraction rather than scattering pricing constants
throughout application code. The frontend need not provide an elaborate
FinOps dashboard — expose backend APIs and demonstrate retrieval.

## MINIMAL WEB APPLICATION

Build a lightweight web interface. The frontend is not the primary goal
— it should prove backend integration. Provide pages/views for: system
health, recent events, model metadata, pipeline runs, incidents, agent
action proposals. The UI must call backend APIs. Do not duplicate
business logic in the frontend.

## PYTHON SDK

Create `sdk/python`. Provide a client such as `AMELClient` with typed
methods for: health, predictions, events, models, incidents, agents. Use
the platform REST API. Provide synchronous API initially; optional async
support may follow. Package it properly.

## CLI

Build a Typer-based CLI. Commands should include examples such as: `amel
health`, `amel predict`, `amel models list`, `amel events list`, `amel
incidents list`, `amel agent diagnose`. The CLI should use the SDK rather
than duplicate HTTP logic.

## DOCKER

Every first-party runtime service needs an appropriate Dockerfile. Build
optimized images with sensible layering. Use non-root users where
practical. Implement health checks. Create a local Docker Compose
environment. The system must support a documented command similar to
`docker compose up -d` followed by migration/bootstrap commands. Provide
`make up`, `make down`, `make logs`, `make test`, `make migrate`, `make
seed`, `make smoke` where appropriate.

## KUBERNETES

Only after Docker Compose works, create Kubernetes deployment manifests.
Support local clusters such as kind or minikube. Use a namespace.
Implement: Deployments, Services, ConfigMaps, Secrets, Jobs, Ingress,
resource requests/limits, liveness probes, readiness probes, startup
probes where appropriate. Do not place secrets directly in committed
manifests.

## SCALING

Implement an autoscaling demonstration. Use Kubernetes HPA for the
inference API. Later implement KEDA for Kafka consumers so consumer
replicas can respond to Kafka backlog/lag. Document: horizontal scaling,
consumer groups, Kafka partitions, maximum useful consumer concurrency.
Demonstrate that adding more consumer replicas than partitions does not
increase useful partition consumption.

## CI/CD

Use GitHub Actions. Create workflows for: lint, format check, type
check, unit tests, integration tests, container builds, security
scanning where reasonable, artifact generation, Kubernetes manifest
validation. PRs must run validation. Main branch builds deployable
images. Do not deploy automatically to a real production environment
without explicit configuration. Use commit SHAs for traceability.

## TESTING

Implement unit tests, integration tests, contract tests, end-to-end smoke
tests, using pytest. Integration tests should cover real interactions
where feasible. Important scenarios include: duplicate Kafka delivery,
invalid event → DLQ, database rollback, consumer restart, API
authentication, RBAC denial, model unavailable, feature unavailable,
MinIO unavailable, Kafka unavailable, agent permission denial, webhook
invalid signature. Do not claim completion while core tests are failing.

## FAILURE ENGINEERING

Provide scripts or documented exercises that deliberately break the
system. Examples: stop PostgreSQL, stop Kafka, kill consumer halfway
through processing, send duplicate events, send malformed events, remove
MLflow model, cause API timeout, cause Kubernetes pod crash, produce
Kafka backlog. For every failure exercise document: expected failure,
observable symptom, metric/log/trace, recovery behavior, data-loss
implications, manual remediation.

## SERVERLESS ARCHITECTURE

After Kubernetes deployment works, document and implement one small
serverless target. The application architecture must remain
cloud-neutral. Potential candidates: inference API, webhook receiver,
metadata API. Do not attempt to make Kafka, PostgreSQL, Kubeflow or other
major stateful systems artificially serverless. Document how local
services map to managed equivalents. Use adapters/configuration rather
than rewriting business logic.

## DOCUMENTATION REQUIREMENTS

Maintain: `README.md`, `ARCHITECTURE.md`, `PROJECT_STATE.md`,
`LEARNING_LOG.md`, `DECISIONS.md`, `SECURITY.md`, `RUNBOOKS.md`.

`PROJECT_STATE.md` must contain: current milestone, completed work,
current failures, commands that work, next implementation task. This
exists so another Claude Code session can resume without needing complete
conversational history.

`DECISIONS.md` should contain Architecture Decision Records. For
significant choices document: problem, options, decision, reason,
tradeoffs, future reconsideration trigger.

## LEARNING REQUIREMENT

This repository is explicitly educational. After implementing every major
subsystem, update `LEARNING_LOG.md` with: WHAT WAS BUILT, WHY IT EXISTS,
HOW DATA/CONTROL FLOWS THROUGH IT, IMPORTANT CODE FILES, FAILURE MODES,
HOW TO TEST IT, CONCEPTS THE DEVELOPER SHOULD UNDERSTAND, INTERVIEW
QUESTIONS THAT COULD BE ASKED. Do not merely explain syntax — explain
engineering concepts.

## CODE QUALITY

Use Python 3.12+ unless a dependency requires otherwise (see the Python
3.14 caution above — resolve before Milestone 1 if it blocks a core
dependency), type hints, Pydantic, SQLAlchemy 2 style, Alembic, pytest,
Ruff, mypy where practical. Avoid enormous `service.py` files. Separate:
API/router layer, schema layer, service/business layer,
repository/data-access layer, infrastructure/adapters. Do not introduce
abstractions without a reason. Avoid circular imports. Use dependency
injection where it materially improves testability.

---

## BUILD ORDER

Do **not** attempt to build everything simultaneously. Build and verify
in this order. Every milestone below must satisfy the Definition of Done
checklist in addition to its own acceptance criteria.

**MILESTONE 0** — Repository skeleton, documentation conventions, Python
tooling, GitHub remote.
Acceptance: repo exists on GitHub at
`chetankumar-works/agentic-ml-engineering-lab` with the initial commit
pushed; `README.md`, `ARCHITECTURE.md`, `PROJECT_STATE.md`,
`LEARNING_LOG.md`, `DECISIONS.md`, `.env.example`, `pyproject.toml`,
`Makefile`, `.gitignore` all exist and are non-placeholder; `pyproject.toml`
configures ruff/mypy/pytest; `make` targets exist (may be stubs pointing
at "not yet implemented" for infra not built yet, but must not error);
verified tool versions and the Python-version decision are recorded in
`PROJECT_STATE.md` and, if relevant, `DECISIONS.md`.

**MILESTONE 1** — PostgreSQL, Kafka, source simulator, stream consumer.
Acceptance: 100,000+ events can be streamed and persisted. Duplicates do
not create duplicate records. Malformed events reach DLQ. Consumer
restart is safe.

**MILESTONE 2** — Airflow + MinIO + bronze/silver/gold processing.
Acceptance: a scheduled DAG processes newly arrived data idempotently.

**MILESTONE 3** — Feast + Redis.
Acceptance: historical features can be retrieved and online
materialization demonstrated.

**MILESTONE 4** — Training package + MLflow.
Acceptance: decision-tree training run is reproducible and model
registered.

**MILESTONE 5** — FastAPI inference.
Acceptance: API loads champion model and returns predictions with model
metadata.

**MILESTONE 6** — OpenTelemetry and observability stack.
Acceptance: a single prediction can be traced across relevant service
boundaries.

**MILESTONE 7** — Docker hardening + CI/CD.
Acceptance: clean checkout can start the local environment using
documented commands, and GitHub Actions passes on the pushed branch.

**MILESTONE 8** — Kubernetes.
Acceptance: core stateless services run on kind/minikube with health
probes and configuration separation.

**MILESTONE 9** — Kubeflow Pipelines v2.
Acceptance: training executes as containerized KFP components and
records results in MLflow.

**MILESTONE 10** — HPA + KEDA scaling experiment.
Acceptance: inference can scale and Kafka consumer scaling can be
demonstrated.

**MILESTONE 11** — Metadata graph + normalized platform events +
webhooks.
Acceptance: resources and relationships can be queried from the platform
API.

**MILESTONE 12** — Agent supervisor + tools + RAG + policy engine.
Acceptance: agent can investigate an incident using read-only tools and
produce a structured plan.

**MILESTONE 13** — Temporal + sandbox + approval workflow.
Acceptance: a remediation workflow can pause for approval and resume
after restart.

**MILESTONE 14** — MCP interface.
Acceptance: external compatible clients can invoke approved read-only
platform tools.

**MILESTONE 15** — SDK + CLI + minimal web client.
Acceptance: all three use the same API contracts.

**MILESTONE 16** — FinOps + serverless experiment + final documentation.

## CRITICAL EXECUTION RULES

Do not mark a milestone complete merely because files were created — use
the Definition of Done checklist above, every time, including step 12
(push + tag). Prefer working vertical slices over huge speculative
architecture. Do not silently ignore errors. Do not delete failing tests
to achieve green CI. Do not replace requested technology merely because
another tool is easier, without documenting and discussing the
architectural reason in `DECISIONS.md`. When a dependency version has
changed, inspect its current official documentation rather than assuming
older syntax.

## TEACHING BEHAVIOR

When implementing a major concept, include a concise explanation for the
developer. Whenever an important architecture choice appears, explain:
why this component exists, what would happen without it, what
alternatives exist, why this implementation was chosen. When generating
non-trivial code, make the final structure understandable enough that the
developer can defend it during an interview. The goal: AI writes much of
the implementation; the developer understands and can explain the
architecture, data flow, reliability properties and important code.

## INITIAL ACTION

Begin with Milestone 0 only.

1. Inspect the development environment (re-verify the versions listed
   above; resolve the Python 3.14 compatibility question and record the
   decision).
2. Create the initial repository structure and required docs listed in
   Milestone 0's acceptance criteria.
3. Verify `gh auth status`, then create/connect the GitHub remote at
   `chetankumar-works/agentic-ml-engineering-lab` (public), commit, and
   push.
4. Propose the exact local dependency versions and Docker architecture in
   `DECISIONS.md`.
5. Then implement Milestone 1.

Do not generate all later components as placeholders. We will grow the
architecture from a working data-streaming foundation.
