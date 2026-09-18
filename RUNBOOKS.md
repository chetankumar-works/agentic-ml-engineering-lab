# Runbooks

Operational runbooks for AMEL failure scenarios. Each entry follows the
same shape so the Incident Agent (Milestone 12+) can retrieve and reason
about them, and so a human operator can act on them without other
context:

```
## <failure scenario>
- Symptom: what's observably wrong (metric, log, error, alert)
- Likely causes: ranked list
- Diagnosis steps: exact commands/queries to run
- Recovery: exact steps to restore service
- Data-loss implications: what, if anything, is unrecoverable
- Prevention/follow-up: what to fix so it doesn't recur
```

No runbooks exist yet — there are no runtime services to fail. The
Failure Engineering exercises specified in `AMEL_KICKOFF_PROMPT.md`
(stop Postgres, stop Kafka, kill a consumer mid-batch, duplicate/malformed
events, remove the MLflow model, API timeouts, pod crashes, Kafka
backlog) will each get a runbook entry here as their corresponding
milestone is built and the failure is deliberately exercised — not written
speculatively ahead of the system existing.
