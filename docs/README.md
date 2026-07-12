# iceops documentation

Doctor, janitor, and autopilot for your Apache Iceberg lakehouse — one `pip install`, no
JVM, no cluster, no platform to deploy.

## Start here

- **[Quickstart](quickstart.md)** — install and see value in five minutes
- **[Concepts](concepts.md)** — what each maintenance operation actually does, in plain
  language (small files, snapshots, manifests, orphans)
- **[Command reference](commands.md)** — every command, its options, and example output
- **[Policy (`iceops.yaml`)](policy.md)** — per-table maintenance as code with `iceops apply`
- **[Engines](engines.md)** — native vs. Spark / Trino execution, and how to configure them

## The mental model

iceops is one loop: **make the state of every Iceberg table observable, and every change
to it reviewable.**

```
scan ──▶ plan ──▶ review ──▶ apply ──▶ verify ──▶ (back to scan)
```

| Stage | What it means | In iceops |
| --- | --- | --- |
| **scan** | observe reality, grade it, price it | `iceops scan` / `doctor` / `cost` |
| **plan** | a literal listing of what *would* change | every fix command's default is a dry run |
| **review** | a human decides — non-removable | you read the plan; or your team reviews the `iceops.yaml` PR |
| **apply** | execute exactly the reviewed plan | `--yes` (per command) / `iceops apply` |
| **verify** | confirm the loop converged | re-run `scan` — status flips; exit codes make it CI-checkable |

Tables keep getting written to, so it's a cycle, not a pipeline — the same
`plan → review → apply` discipline as terraform, pointed at table health.
