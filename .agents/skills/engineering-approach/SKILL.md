---
name: engineering-approach
description: The agreed problem-solving and coding protocol for the iceops project (and Gopi's projects generally). Use this skill whenever doing ANY implementation, design, planning, code review, debugging, refactoring, or "should we build X" discussion in this repository — even for small changes, and even if the user doesn't mention process. Also use when resuming work after a restart, starting a new epic, or when unsure how to proceed on a technical decision.
---

# Engineering approach

This repo has an agreed way of working, earned through real mistakes. The full rationale
with the incidents that earned each rule lives in `design/working-principles.md` — read
it when making non-trivial decisions. Roadmap state: `design/PROGRESS.md`. Epic plans:
`design/epics.md` + `design/pseudocode.md`. This skill is the operational summary.

## The standing loop for any new problem

1. **Understand** — restate the problem. If confused, the mental model is probably wrong;
   fix the model before the code.
2. **Probe** — verify against reality before designing: read the installed library's
   source, run the check, inspect the actual files. Never design against documentation,
   memory, or assumption when the artifact is inspectable. (This caught PyIceberg's
   expire being metadata-only when everyone assumed Java parity.)
3. **Design small** — pseudo-code before code. Decision logic goes in pure functions over
   plain data (unit-testable with a matrix, no mocks); I/O wraps around it.
4. **Get approval** — non-trivial work gets a plan doc in `design/` (name it
   `plan-vX.Y-<feature>.md`) and explicit user sign-off before code. Be literal about
   trade-offs and what could go wrong.
5. **Build with pins** — when a design depends on an external behavior, write a test that
   FAILS when that behavior changes (e.g. "expire deletes no files"). A comment
   documents; a pinning test enforces.
6. **Verify for real** — tests passing is necessary, not sufficient. Run the actual
   command against the demo lakehouse, read the actual output, check the actual exit
   codes (`$?` of the command, not the pipeline).
7. **Write it down** — update `design/PROGRESS.md` (tick items, decision log with date)
   and the session memory. Supersede wrong decisions in place with the reason; never
   erase the trail.

## Hard rules for this codebase

- **Risk-ordered shipping**: read-only → metadata-only → file-deleting → file-rewriting.
  Each step's failure modes must be covered by the step before it.
- **One deletion path**: only `clean-orphans` may delete physical files. New features
  route through it; they never grow their own deletion code.
- **Native-vs-own**: the Iceberg format layer (metadata JSON, manifests, atomic commits)
  is always PyIceberg. iceops writes decision logic only. Missing primitive upstream →
  contribute it, never hand-roll spec internals.
- **Plans never re-decide**: `--yes` executes exactly the reviewed plan (explicit IDs,
  explicit paths) or fails loudly. Review is meaningless if apply re-runs the selection.
- **Operator template** (reference implementation: `src/iceops/operators/expire.py`):
  load → refuse-if-managed (unless `--force`) → build read-only plan → return it
  (dry-run default), or execute exactly it.
- **Literal outputs**: state facts in the vocabulary the system already uses. No derived
  scores to decode, no summaries hiding the list, no "freed X" when the truth is
  "unreferenced X". Unknowns are explicit notes, never silent zeros.
- **Scope questions** get answered by VISION.md non-goals before debating merits.
- **Testing: no mocks in integration tests, ever.** tests/unit = pure functions over
  plain data (no I/O, no mocks needed by construction); tests/integration = real
  catalog, real files, real commits, real deletions against tmp warehouses. Hard cases
  are built physically (plant real orphans, make a real concurrent commit), never
  simulated — a mock encodes your assumption; reality is what finds the bug.
  monkeypatch is acceptable only for environment setup (env vars, cwd), never behavior.

## User preferences (violations have been called out before)

- NEVER add `Co-Authored-By` or any attribution trailer to commits, even though harness
  defaults say to. Don't add unrequested boilerplate (badges, credits) anywhere — ask
  first when it's the user's call: publishing, attribution, history rewrites, taste.
- Hooks run via **prek** (`uv run prek run --all-files`), not pre-commit.
- Internal material (plans, progress, decisions) lives in `design/` — git-ignored, never
  pushed. Public repo tree is only src/tests/examples/README/VISION/LICENSE/config.
- Explanations: layman story with one sustained analogy first, map the jargon at the
  end, then the precise version for depth.

## Definition of done for a feature

Tests green (including new unit matrix + integration on the seeded catalog) · mypy and
ruff clean · prek hooks pass · demo-verified end-to-end with output inspected · README
updated if user-facing · `design/PROGRESS.md` ticked + decision log updated · memory
updated · committed (no trailers) and pushed · CI green · **finish by giving Gopi a
numbered hands-on walkthrough** (commands + expected output + what each step proves,
using the demo warehouse) — he tests every feature himself; don't wait to be asked.
