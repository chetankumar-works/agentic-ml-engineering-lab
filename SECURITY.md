# Security

AMEL is a learning project but is treated with real security discipline —
the point is to practice the actual habits, not simulate them.

## Secrets

- Never commit real credentials, tokens, keys, or connection strings.
- `.env.example` documents every required environment variable with a
  placeholder value only. Copy it to `.env` (gitignored) for local use.
- No first-party service logs secret values. If you find one that does,
  that's a bug — fix it, don't suppress the log line.
- Local development authentication (Milestone 5+) uses self-signed JWTs
  for convenience; this is explicitly a development-only mechanism and is
  documented as such wherever it's implemented.
- Kubernetes Secrets (Milestone 8+) replace `.env` files for in-cluster
  config; a documented migration path toward Vault or a cloud secret
  manager is noted at that point rather than implemented, since this is a
  local/learning deployment.

## AuthN / AuthZ model **(planned, lands with `apps/platform_api` and `apps/inference_api`)**

- Roles: `viewer`, `developer`, `operator`, `admin`.
- Scopes: `resources:read`, `events:read`, `models:read`,
  `models:promote`, `agents:execute`, `deployments:write`, `admin:all`.
- State-changing agent actions require explicit authorization and pass
  through policy evaluation before execution (see the Agent Safety
  section of `ARCHITECTURE.md` / `AMEL_KICKOFF_PROMPT.md` once the agent
  control plane is built).

## Agent action safety **(planned, Milestone 12+)**

Risk classifications (`READ_ONLY`, `SANDBOX_WRITE`, `REPOSITORY_WRITE`,
`INFRASTRUCTURE_WRITE`, `DESTRUCTIVE`) gate what an agent may do
autonomously versus what requires human approval. Every requested action
— agent, user, permissions, decision, execution result, timestamp — is
persisted to audit storage (`audit.audit_events`), not just logged.

## Reporting

This is a personal learning project with no external users; there is no
formal disclosure process. If you (a future session or the developer)
find a credential accidentally committed, rotate it immediately and scrub
it from history, then record what happened and the fix in
`LEARNING_LOG.md` — that failure mode (secret leaked into git history) is
itself worth understanding.
