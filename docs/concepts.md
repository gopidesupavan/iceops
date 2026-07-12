# Concepts — what iceops actually does

Iceberg tables get slower and more expensive over time, not because the format is bad but
because of the operational debris that piles up around it. This page explains each problem
in plain language and which iceops command fixes it.

## The four problems

### Small files
Every write — every append, every streaming micro-batch — creates data files. A table
written once a minute accumulates thousands of tiny files. Queries pay an "open cost" per
file, so many small files make every query slower for no benefit.
→ **Fix: `iceops compact`** merges small files into fewer, larger ones.

### Snapshot bloat
Every commit creates a **snapshot** — a frozen view of the table at that moment. Iceberg
never deletes them on its own. They power time travel and rollback, but they pile up: they
grow metadata, and — the sneaky part — they keep old data files alive on storage. When you
compact 500 small files into 5, the old 500 don't disappear, because yesterday's snapshot
still points at them. You pay for both copies until the snapshot is expired.
→ **Fix: `iceops expire`** forgets old snapshots (keeps the newest N and everything within
a time window). You lose the ability to time-travel to the forgotten versions — that's the
trade.

### Manifest fragmentation
A snapshot points at **manifests** — index files listing the data files and their stats.
Every append adds a manifest. 60 appends = 60 tiny manifests, and query *planning* reads
every one before touching data. It's a phone book split into 60 booklets.
→ **Fix: `iceops rewrite-manifests`** consolidates them into few large manifests.

### Orphan files
Failed writes, and the files that compaction/expiry leave behind, become **orphans** —
files in the table's storage that no snapshot references. They're invisible to queries but
they still cost storage money.
→ **Fix: `iceops clean-orphans`** deletes them. This is the only iceops command that
deletes physical files, and it is built paranoid (see below).

## How they connect

The problems compound in a specific order, which is why `tune` runs the fixes in this
order:

```
compact ──▶ rewrite-manifests ──▶ expire ──▶ clean-orphans
```

1. **compact** merges small files → the old small files become referenced only by old
   snapshots (stale).
2. **rewrite-manifests** consolidates the manifests compact just produced.
3. **expire** drops old snapshots → unreferences the pre-compaction files.
4. **clean-orphans** physically reclaims everything now unreferenced.

Run them out of order and you either do useless work or leave storage unreclaimed. `tune`
and `apply` encode this order so you can't get it wrong.

## Two things worth understanding

### expire deletes no files — it only unreferences them
Expiration is a metadata operation: it removes old snapshots from the table's metadata.
The now-unreferenced data files stay on disk until `clean-orphans` removes them. So
reclaiming space is always a two-step dance: `expire` (or `compact`) makes files
unreferenced, then `clean-orphans` reclaims them — and only after they age past its safety
window. A single maintenance pass won't reclaim everything immediately; that's intentional.

### clean-orphans is deliberately cautious
Because it deletes real files in production data lakes, it:
- deletes only files referenced by **no** snapshot (drawn from six metadata sources),
- **never** deletes `*.metadata.json` (the audit/undo chain),
- **never** deletes files younger than `--older-than` (default 3 days — a fresh
  unreferenced file may be an in-flight write),
- supports `--exclude` globs, and
- re-checks the table before every delete batch in case a writer committed mid-run.

Its worst-case failure mode is deleting *too little*, never too much.

## Health status

`doctor` and `scan` report a table's status as the **worst finding severity** — `healthy`,
`warn`, or `critical`. There's no letter grade to decode; the status uses the same words as
the findings. `info` findings are advice and never change the status.

## Cost model

`cost` classifies every byte into three buckets: **live** (referenced by the current
snapshot — your table), **stale** (only in old snapshots — freed by `expire`), and
**orphan** (referenced by nothing — freed by `clean-orphans`). The monthly-waste figure is
`(stale + orphan) × price`, and it's honest about what it can't measure: unknowns are shown
as notes, never silently zeroed, so the total is a floor.
