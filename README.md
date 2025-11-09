# Concurrent Index Creation Study

Test `CREATE INDEX CONCURRENTLY` (`CIC`) scenarios to observe how a follow-up
`CREATE INDEX IF NOT EXISTS` behaves at different catalog states (no entry, not
ready, ready-but-invalid, validation phase, steady state). It spins up
disposable Postgres containers, seeds data, runs CIC, then issues the
non-concurrent variant while writers/probes keep pressure on the table. Logs are
timestamped relative to each scenario to compare durations linearly.

## TL;DR

- Once a concurrent build fully commits (`sequential`), the follow-up
  `CREATE INDEX IF NOT EXISTS` is a clean no-op.
- If the non-concurrent command starts before a catalog entry exists
  (`before-catalog`), it builds the index outright while holding the usual
  `RowExclusiveLock`; CIC wakes up, sees a ready+valid index, and exits
  immediately.
- As soon as the catalog row exists but CIC is still building or waiting to
  validate (`exists-not-ready`, `ready-not-valid`), overlapping creation almost
  always trips Postgres’ deadlock detector. In **most** runs the database
  cancels the concurrent build and lets the non-concurrent build finish; on rare
  runs the cancellation flips the other way.
- When we wait until CIC is actively inside the validation phase (scenario
  `validation-phase` the IF NOT EXISTS call now succeeds most of the time
  because the concurrent session already holds the heavier locks it needs.
  Deadlocks can still happen, but they are now the exception rather than the
  rule.

## Scenarios

| `key`              | Explanation                                                                                                                                         |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sequential`       | Baseline run: finish CIC so the index is ready+valid, then immediately issue `CREATE INDEX IF NOT EXISTS` to confirm it no-ops.                     |
| `before-catalog`   | Overlap where the non-concurrent command starts before CIC creates a catalog entry, so `idx=<missing>` shows up until the plain build finishes.     |
| `exists-not-ready` | Collides with CIC while it is still scanning/sorting; catalog row exists but `indisready=false`, so IF NOT EXISTS can block on share-update locks.  |
| `ready-not-valid`  | Triggers once CIC finished building (`indisready=true`) but before validation flips `indisvalid`, capturing the gap between build and validation.   |
| `validation-phase` | Fires during CIC’s validation pipeline (scanning index/table, waiting for snapshots) to show how IF NOT EXISTS behaves during that critical window. |

Run `go run . [scenario-key]` to focus on a subset; omit args to execute all.

## Observed CIC Phases

### Non-concurrent Starts First (`before-catalog`)

1. `idx=<missing> progress=building index: scanning table`

   The concurrent worker starts, but still reports the index as missing while it
   parses and scans the table.

2. `idx=<missing> progress=building index: sorting live tuples`

   The concurrent worker advances to sorting while the non-concurrent build
   continues to hold the lead.

3. `idx ready=true valid=true progress=<inactive>`

   The concurrent session wakes up, notices the index is already valid, and
   exits without entering the validation phase.

### CIC Starts First

_Note_: not each progress change shows up on every run (despite 1ms polling).

1. `idx=<missing> progress=building index: scanning table`

   No catalog row yet; CIC is still parsing/scanning.

2. `idx ready=false valid=false progress=building index: scanning table`

   A catalog entry exists but the scan is ongoing.

3. `idx ready=false valid=false progress=building index: sorting live tuples`

   Scan completed; tuples are being sorted.

4. `idx ready=false valid=false progress=building index: loading tuples in tree`

   Sorted tuples are inserted into the index structure.

5. `idx ready=true valid=false progress=waiting for writers before validation`

   Build phase is done; waiting to start validation while writers drain.

6. `idx ready=true valid=false progress=index validation: scanning index`

   Validation begins by scanning the new index.

7. `idx ready=true valid=false progress=index validation: sorting tuples`

   Validation sorts tuples for comparison.

8. `idx ready=true valid=false progress=index validation: scanning table`

   Validation cross-checks rows against the table.

9. `idx ready=true valid=false progress=waiting for old snapshots`

   Validation is done; waiting for old snapshots before commit.

10. `idx ready=true valid=false progress=<inactive>`

    The CIC session finished but `indisvalid` hasn’t flipped yet.

### Index Creation Phases (`pg_stat_progress_create_index.phase`)

_Note_: the phases listed are those observed. A full list can be found in the
[postgres docs](https://www.postgresql.org/docs/current/progress-reporting.html#CREATE-INDEX-PHASES).

1. `building index: scanning table`
2. `building index: sorting live tuples`
3. `building index: loading tuples in tree`
4. `waiting for writers before validation`
5. `index validation: scanning index`
6. `index validation: sorting tuples`
7. `index validation: scanning table`
8. `waiting for old snapshots`

## Example Output of `ready-not-valid` Run

```
[+0.0000s] ==== Scenario 4: state: ready but not valid ====
[+0.0000s] [ready-not-valid] starting Postgres 16.8 container
[+1.8921s] [ready-not-valid] Postgres up at postgres://u:pw@localhost:33221/db?sslmode=disable
[+1.9036s] [ready-not-valid] creating table t
[+3.1639s] [ready-not-valid] seed of 1000000 rows completed in 1257.20 ms
[+3.1640s] [ready-not-valid] launching CREATE INDEX CONCURRENTLY in background
[+3.1640s] [ready-not-valid] waiting for catalog entry that is ready but not yet valid
[+3.1708s] [cic] started
[+3.1713s] [writers] started
[+3.1715s] [probe] started
[+3.1720s] [probe] insert #01 completed after 0.52 ms
[+3.1728s] [cic] snapshot updated: idx ready=false valid=false progress=building index: scanning table
[+3.2166s] [cic] snapshot updated: idx ready=false valid=false progress=building index: sorting live tuples
[+3.2724s] [probe] insert #02 completed after 0.92 ms
[+3.3721s] [probe] insert #03 completed after 0.46 ms
[+3.4720s] [probe] insert #04 completed after 0.37 ms
[+3.5746s] [probe] insert #05 completed after 0.58 ms
[+3.6719s] [probe] insert #06 completed after 0.39 ms
[+3.7724s] [probe] insert #07 completed after 0.86 ms
[+3.8738s] [probe] insert #08 completed after 0.95 ms
[+3.9720s] [probe] insert #09 completed after 0.37 ms
[+4.0719s] [probe] insert #10 completed after 0.39 ms
[+4.1718s] [probe] insert #11 completed after 0.32 ms
[+4.2726s] [probe] insert #12 completed after 1.05 ms
[+4.3287s] [cic] snapshot updated: idx ready=false valid=false progress=building index: loading tuples in tree
[+4.3748s] [probe] insert #13 completed after 2.84 ms
[+4.4740s] [probe] insert #14 completed after 2.36 ms
[+4.5398s] [cic] snapshot updated: idx ready=false valid=false progress=waiting for writers before validation
[+4.5418s] [cic] snapshot updated: idx ready=true valid=false progress=waiting for writers before validation
[+4.5418s] [ready-not-valid] IF NOT EXISTS trigger: idx ready=true valid=false
[+4.5418s] [ready-not-valid] starting non-concurrent CREATE INDEX IF NOT EXISTS idx
[+4.5437s] [ready-not-valid] locks before non-concurrent build: RowExclusiveLock, ShareUpdateExclusiveLock
[+4.5628s] [cic] snapshot updated: idx ready=true valid=false progress=index validation: scanning index
[+4.5878s] [cic] snapshot updated: idx ready=true valid=false progress=index validation: sorting tuples
[+4.6282s] [cic] snapshot updated: idx ready=true valid=false progress=index validation: scanning table
[+4.6556s] [cic] snapshot updated: idx ready=true valid=false progress=waiting for old snapshots
[+5.5510s] [probe] insert #15 completed after 979.40 ms
[+6.5768s] [ready-not-valid] non-concurrent build finished in 2033.05 ms
[+6.5772s] [cic] failed after 3406.35 ms: ERROR: deadlock detected (SQLSTATE 40P01)
[+6.5821s] [probe] insert #16 completed after 1031.03 ms
[+6.5875s] [probe] insert #17 completed after 5.40 ms
[+6.5876s] [ready-not-valid] locks after non-concurrent build: RowExclusiveLock
[+6.5899s] [cic] snapshot updated: idx ready=true valid=false progress=<inactive>
[+6.6009s] [probe] stopping
[+6.6011s] [writers] stopping
[+6.8658s] [ready-not-valid] finished with error: ERROR: deadlock detected (SQLSTATE 40P01)
```
