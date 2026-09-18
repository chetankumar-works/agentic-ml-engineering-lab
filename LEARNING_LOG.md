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
